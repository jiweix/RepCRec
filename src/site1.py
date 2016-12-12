# -----------------------------------------------------------------------------
# site1.py
#
# Classes for sites
# -----------------------------------------------------------------------------

import time
import bisect
import logging
from enum import Enum
import transaction
from lock import FIFOLock, Mode


class Status(Enum):
    failed = 1
    running = 2


class Site(object):
    """
    Site object for maintaining data and locks.
    """
    def __init__(self, tm, idx):
        """
        Create one site that manages data items and their locks.
        Historical values are tracked for read only transactions
        Write values are cached, only write the data item if transactions can commit
        Site fail and recovery time is stored, which is used to check if a transaction should commit

        :param tm:  the global Transaction Manager
        :param idx: site id
        """
        self._tm = tm
        self.idx = idx
        self.status = Status.running
        self.lock_table = dict()
        self.historical_timestamps = dict()
        self.historical_values = dict()
        self.uncommitted_values = dict()
        self.breakpoints = [self._tm.timestamp]

    @property
    def last_timestamp(self):
        return self.breakpoints[-1]

    def fail(self):
        """
        Site fails
        The fail time is recorded. This will be used when a transaction trying to commit
        """
        self.status = Status.failed
        self.breakpoints.append(self._tm.timestamp) # add a breakpoint
        logging.debug('site %d is failed' % self.idx)

    def recover(self):
        """
        Site recovers
        reset all locks
        reset all uncommitted values
        add recovery time stamp
        """
        self.status = Status.running
        self.lock_table = dict() # all locks were lost
        self.uncommitted_values = dict() # all uncommitted were lost
        self.breakpoints.append(self._tm.timestamp) # add a breakpoint
        logging.debug('site %d is recovered' % self.idx)

    def read(self, t, x, ts=None):
        """
        Try to read a variable x by transaction t
        If the transaction is read/write transaction, and if the read is successful, the transaction would
        get the read lock the the variable x on this site.

        :param t:   transaction that try to read the variable
        :param x:   the variable to read
        :param ts:  the time stamp for readonly transactions
        :return:    None, None  if site is down
                    Set, None   if the transaction need to wait because the item is locked.
                                the Set contains transactions to wait for
                    True, val   if succeed. the val is the value by perform read.
        """
        if ts is None:
            ts = self._tm.timestamp
        assert self.status == Status.running
        if isinstance(t, transaction.ReadOnlyTransaction):
            logging.debug('ts = %d' % ts)
            logging.debug(str(self.historical_timestamps[x.name]))
            logging.debug(str(self.historical_values[x.name]))
            # read from persistent data structure
            # should return (ts, val)
            i = bisect.bisect_left(self.historical_timestamps[x.name], ts) - 1
            assert i >= 0
            # ignore availability if it is the only site
            if len(x.sites) == 1:
                return (True,
                    self.historical_values[x.name][i])
            # check availability
            j = bisect.bisect_left(self.breakpoints, ts) - 1
            assert j >= 0
            if j % 2 == 1: # even number of breakpoints: failed at that time
                return False, None
            elif self.historical_timestamps[x.name][i] < self.breakpoints[j]:
                return False, None
            return (True,
                self.historical_values[x.name][i])
        else:
            if self._initialized(x):
                ret = self._acquire_lock(t, x.name, mode=Mode.read)
                if ret is True:
                    # success
                    if x.name in self.uncommitted_values:
                        return True, self.uncommitted_values[x.name][-1]
                    else:
                        return True, self.historical_values[x.name][-1]
                else:
                    # failed
                    return ret, None
            else:
                # act as if down
                return None, None

    def write(self, t, x, val):
        """
        Try to write a variable x with val by transaction t
        If the operation is successful, the transaction would get write lock on variable x on this site.

        :param t:   transaction that try to write
        :param x:   the variable to write
        :param val: the value to write
        :return:    True if succeed, else return the set of transaction to wait for getting the lock
        """
        assert self.status == Status.running
        assert not isinstance(t, transaction.ReadOnlyTransaction)
        if self._initialized(x):
            ret = self._acquire_lock(t, x.name, mode=Mode.write)
            if ret is True:
                # success
                self.uncommitted_values[x.name] = t, val
                return True
            else:
                return ret
        else:
            # use write to initialize
            assert x.name not in self.lock_table
            ret = self._acquire_lock(t, x.name, mode=Mode.write)
            assert ret
            self.uncommitted_values[x.name] = t, val
            return True

    def available(self, ts):
        """
        Check if a site is running at time ts

        :param ts:  time stamp to check
        :return:    True if site is running at time ts
        """
        assert self.status == Status.running
        return ts > self.last_timestamp # caution: > or >=?

    def commit(self, t):
        """
        Write uncommitted values from transaction t to historical values
        Clear the locks hold by t
        Clear the cached uncommitted values by t

        :param t:   The transaction to commit
        """
        self._clean(t, write=True)

    def abort(self, t):
        """
        Clear the locks hold by t
        Clear the cached uncommitted values by t

        :param t:   The transaction to abort
        """
        self._clean(t, write=False)

    def _clean(self, t, write=False):
        assert self.status == Status.running
        # write into persistent data structure
        r = []
        for x in self.uncommitted_values:
            owner, val = self.uncommitted_values[x]
            if owner is t:
                if write is True:
                    self._archive(x, self._tm.timestamp, val)
                r.append(x)
        for x in r:
            del self.uncommitted_values[x]
        # clean lock table
        self._release_lock(t)

    def _archive(self, name, ts, val):
        if name not in self.historical_values:
            self.historical_timestamps[name] = [ts]
            self.historical_values[name] = [val]
        else:
            # ensure times grows
            if len(self.historical_timestamps) > 0:
                assert self.historical_timestamps[name][-1] < ts
            self.historical_timestamps[name].append(ts)
            self.historical_values[name].append(val)

    def _acquire_lock(self, t, x, mode):
        if x not in self.lock_table:
            self.lock_table[x] = FIFOLock()
        return self.lock_table[x].acquire(t, mode)

    def _release_lock(self, t):
        for x in self.lock_table:
            self.lock_table[x].release(t)

    def _initialized(self, x):
        if x.name in self.uncommitted_values:
            return True
        if len(x.sites) == 1:
            return True
        if x.name not in self.historical_values:
            return False
        i = bisect.bisect_left(self.historical_timestamps[x.name],
                self.last_timestamp)
        return i < len(self.historical_timestamps[x.name])
