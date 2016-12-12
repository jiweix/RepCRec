# -----------------------------------------------------------------------------
# lock.py
#
# Classes for locks
# -----------------------------------------------------------------------------

import logging
from enum import Enum
from collections import deque
import transaction


class Mode(Enum):
    read = 1
    write = 2


class LockBase(object):
    """
    Abstract class for lock
    """
    def __init__(self):
        pass

    def acquire(self, t, mode):
        pass

    def release(self, t):
        pass


class FIFOLock(LockBase):
    """
    First in First out lock
    The lock could only be acquired in FIFO manner.
    Read locks are not exclusive, but write lock is
    Lock type is specified as mode (read/write) when acquiring the lock
    Lock object is maintaining by site, there should only one lock per data item
    """
    def __init__(self):
        """
        Create a new lock for the data item
        A set is created to store all holders of the current lock
            for read mode, it allows multiple holders
        A deque is created to track all transactions that are waiting to acquire this lock
        """
        LockBase.__init__(self)
        self.mode = None
        self.holders = set()
        self.queuing = deque()

    def acquire(self, t, mode):
        """
        Try to acquire the lock
        Following the rules that read locks are not exclusive, and FIFO.

        :param t:       The transaction trying to acquire this lock
        :param mode:    type of lock to acquire, read or write
        :return:        True if success, the set of transactions to wait for if not success
        """
        self._maintain_queue()
        if t in self.holders:
            if self._mode_accept(mode):
                # already acquired!
                return True
            # try to upgrade the lock
            if self.mode is Mode.read and mode is Mode.write:
                # the only holder? upgrade in any case, otherwise strange deadlock?
                # t is the first one queuing?
                if len(self.holders) == 1:
                    if self.queuing and t is self.queuing[0]:
                        self.queuing.popleft()
                    self.mode = mode
                    return True
            # has a higher priority, do not queue
            logging.debug(
                'transaction %s cannot upgrade its lock, waiting' % t.name)
            ret = self.holders
        else:
            # R/R
            if self.mode is Mode.read and mode is Mode.read:
                # someone queuing? t is not the first one?
                if not self.queuing:
                    self.holders.add(t)
                    return True
                elif t is self.queuing[0]:
                    self.queuing.popleft()
                    self.holders.add(t)
                    return True
            # no lock holders
            if self.mode is None:
                assert len(self.holders) == 0
                # someone queuing? t is not the first one?
                if not self.queuing:
                    self.mode = mode
                    self.holders.add(t)
                    return True
                elif t is self.queuing[0]:
                    self.queuing.popleft()
                    self.mode = mode
                    self.holders.add(t)
                    return True
            # enqueue
            logging.debug(
                'transaction %s cannot get a new lock, queuing' % t.name)
            assert t not in self.queuing
            # acquired failed
            # queue the transaction
            self.queuing.append(t)
            ret = self.holders | set(self.queuing)
        # return set of transactions to wait for
        # fix bug when this set has transaction itself
        ret = ret.copy()
        ret.discard(t)
        assert len(ret) > 0
        return ret

    def release(self, t):
        """
        Release the lock that one transaction hold
        The transaction is removed from the lock holders
        Lock is reset if no other transaction is holding this lock

        :param t:   The transaction that want to release the lock
        """
        self.holders.discard(t)
        if not self.holders:
            self.mode = None

    def _maintain_queue(self):
        while self.queuing and self.queuing[0].status is transaction.Status.aborted:
            self.queuing.popleft()

    def _mode_accept(self, mode):
        if self.mode is mode:
            return True
        if self.mode is Mode.write:
            return True
        return False
