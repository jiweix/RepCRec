# -----------------------------------------------------------------------------
# transcation.py
#
# Classes for transactions
# -----------------------------------------------------------------------------

from __future__ import print_function
import time
import logging
import site1 as site
from enum import Enum


class Status(Enum):
    created = 1
    ready = 2
    running = 3
    blocked = 4
    committed = 5
    aborted = 6


class Operation(object):
    """
    Single Operation
    Maintains by Transaction.
    Operation id is used to track the sequence order of operations in the input, used to enforce FIFO
    """
    def __init__(self, transaction, id, op, args, kwargs):
        """
        :param transaction: the transaction that maintains this operation
        :param id:          operation id, assigned by the global transaction manager
        :param op:          the operation method
        :param args:        arguments for operation
        :param kwargs:      keywords arguments for operation
        """
        self._t = transaction
        self.id = id
        self.op = op
        self.args = args
        self.kwargs = kwargs


class TransactionBase(object):
    """
    Abstract class for transaction.
    """
    def __init__(self, tm, name, status=Status.created):
        """
        Create a new transaction, record time step from the transaction manager
        Create empty list to maintain all operations as well as results
        Set the pointer to the operations list of next operation to execute

        :param tm:      the global transaction manager
        :param name:    transaction name
        :param status:  default status is created
        """
        self._tm = tm
        self.name = name
        self.status = status
        self.creation_timestamp = self._tm.timestamp
        self.operations = list()
        self.results = list()
        self.extras = list()
        self.next_op_index = 0
    
    @property
    def next_op(self):
        if self.next_op_index >= len(self.operations):
            return None
        return self.operations[self.next_op_index]

    def set_status(self, status):
        logging.info(
            'set transaction %s\'s status from %s to %s' % (
                self.name, self.status, status))
        self.status = status

    def append_operation(self, op, *args, **kwargs):
        """
        Create a new Operation with give arguments and append this Operation to the operation list
        Update next_op and next_op_index if necessary

        :param op:      The operation method
        :param args:    arguments for operation
        :param kwargs:  keywords arguments for operation
        """
        self.operations.append(Operation(self, self._tm.get_op_id(), op, args, kwargs))

    def next_operation(self):
        """
        Performs the next operation
        Update next_op and next_op_index
        """
        assert self.status == Status.running
        if self.next_op is not None:
            self.extra = None
            op, args, kwargs = self.next_op.op, self.next_op.args, self.next_op.kwargs
            ret = op(*args, **kwargs)
            if ret is not False:
                self.results.append(ret)
                self.extras.append(self.extra)
                self.next_op_index += 1

    def read(self, x):
        assert self.status == Status.running
        return False

    def write(self, x, val):
        assert self.status == Status.running
        return False

    def commit(self):
        if self.status is not Status.committed:
            self.set_status(Status.committed)
        # print
        print('%s commits' % self.name)
        # print values read at commit time
        for val, extra in zip(self.results, self.extras):
            if val is not True:
                if (extra is None or 
                    logging.getLogger().getEffectiveLevel() > logging.INFO):
                    print(val)
                else:
                    print(val, extra) # print something else
        return True


class ReadWriteTransaction(TransactionBase):
    """
    Read/write transaction
    At commit time, all sites are checked for their status. If any site fails during this transaction,
    this transaction is aborted.
    """
    def __init__(self, tm, name, status=Status.created):
        """
        Create a new read/write transaction.
        empty sets of transactions that the current transaction is waiting or being waiting for
        is created. This is used for deadlock detection.

        :param tm:      the global transaction manager
        :param name:    transaction name
        :param status:  default status is created
        """
        TransactionBase.__init__(self, tm, name, status=status)
        self.wait_for = set()
        self.waited_by = set()
        self.accessed = list()

    def read(self, x):
        """
        Read variable x from available site

        :param x:   the variable to read
        :return:    return the value that the operation read, False if the operation fails
        """
        assert self.status == Status.running
        for s in x.sites:
            if s.status == site.Status.running:
                ret, val = s.read(self, x)
                if ret is True:
                    # success
                    self.accessed.append((s, self._tm.timestamp))
                    logging.info(
                        'transaction %s reads %s=%d in its %d-th operation' % (
                            self.name, x.name, val, self.next_op_index))
                    logging.debug(
                        'transaction %s accessed site %d at %d' % (
                            self.name, s.idx, self._tm.timestamp))
                    # print
                    logging.info(val)
                    self.extra = '(site = %d, tick = %d)' % (
                        s.idx, self._tm.timestamp)
                    return val
                elif ret is not None:
                    # blocked by other transactions
                    logging.info(
                        'transaction %s fails to read %s '
                        'in its %d-th operation, '
                        'and it is now blocked by %s' % (
                            self.name, x.name, self.next_op_index, 
                            str(map(
                                lambda y: y.name, list(ret)))))
                    self.wait_for.update(ret)
                    logging.debug(
                        'transaction %s\'s wait_for=%s' % (
                            self.name, 
                            str(map(lambda y: y.name, list(self.wait_for)))))
                    for t in ret:
                        t.waited_by.add(self)
                    self.set_status(Status.blocked)
                    return False
                else:
                    # no responds
                    pass
        # all missed, must wait!
        self.set_status(Status.ready)
        return False

    def write(self, x, val):
        """
        Write variable x with value val to all running sites that manage x

        :param x:   variable to write
        :param val: value to write to x
        :return:    True if success, False otherwise.
        """
        assert self.status == Status.running
        written = False
        failed = False
        for s in x.sites:
            if s.status == site.Status.running:
                written = True
                ret = s.write(self, x, val)
                if ret is True:
                    # success
                    self.accessed.append((s, self._tm.timestamp))
                    logging.info(
                        'transaction %s writes %s=%d on site %d '
                        'in its %d-th operation' % (
                            self.name, x.name, val, s.idx, self.next_op_index))
                    logging.debug(
                        'transaction %s accessed site %d at %d' % (
                            self.name, s.idx, self._tm.timestamp))
                elif ret is not None:
                    # blocked by other transactions
                    logging.info(
                        'transaction %s fails to write %s=%d on site %d '
                        'in its %d-th operation, '
                        'and it is now blocked by %s' % (
                            self.name, x.name, val, s.idx, self.next_op_index,
                            str(map(
                                lambda y: y.name, list(ret)))))
                    self.wait_for.update(ret)
                    for t in ret:
                        t.waited_by.add(self)
                    failed = True
                    break
                    # do not exit, try to get as much locks as I can
                else:
                    # should not reach here!
                    assert False
        # failed?
        if failed:
            logging.debug(
                'transaction %s\'s wait_for=%s' % (
                    self.name, 
                    str(map(lambda y: y.name, list(self.wait_for)))))
            self.set_status(Status.blocked)
            return False
        # all success?
        if written:
            logging.info(
                'transaction %s successfuly writes %s=%d '
                'in its %d-th operation' % (
                self.name, x.name, val, self.next_op_index))
        else:
            logging.info(
                'no site is up for transaction %s to write '
                'in its %d-th operation' % (self.name, self.next_op_index))
        return written

    def commit(self):
        """
        Commit a read/write transaction.
        Update other transaction's wait_for and waited_by
        If a transaction is only waiting for this committed transaction, set it status to ready

        :return:    True if commit successes
        """
        logging.info('commit time: transaction %s' % self.name)
        committable = True
        # validation
        for s, ts in self.accessed:
            if s.status == site.Status.running:
                if not s.available(ts):
                    # not still available
                    logging.info(
                        'abort transaction %s at commit time '
                        'because of site %d' % (self.name, s.idx))
                    committable = False
            else:
                # failed
                logging.info(
                    'abort transaction %s at commit time '
                    'because of site %d' % (self.name, s.idx))
                committable = False
        self.set_status(Status.committed if committable else Status.aborted)
        self._clean()
        return True

    def kill(self):
        """
        Kill a read/write transaction.
        Update other transaction's wait_for and waited_by
        If a transaction is only waiting for this committed transaction, set it status to ready

        :return:    True if abort successes
        """
        logging.info('kill: transaction %s' % self.name)
        self.set_status(Status.aborted)
        self._clean()
        return True

    def _clean(self):
        online_sites = set()
        for s, ts in self.accessed:
            if s.status == site.Status.running:
                online_sites.add(s)
        # commit/abort at each site
        for s in online_sites:
            if self.status == Status.committed:
                s.commit(self)
            else:
                s.abort(self)
        # update blocked transactions
        for t in self.wait_for:
            t.waited_by.remove(self)
        for t in self.waited_by:
            t.wait_for.remove(self)
            logging.debug(
                'transaction %s\'s wait_for=%s' % (
                    t.name, str(map(lambda y: y.name, list(t.wait_for)))))
            if len(t.wait_for) == 0:
                t.set_status(Status.ready)
        # print
        if self.status == Status.committed:
            TransactionBase.commit(self)
        else:
            print('%s aborts' % self.name)


class ReadOnlyTransaction(TransactionBase):
    """
    Read Only Transaction
    """
    def read(self, x):
        """
        Read variable x using Multiversion Read Consistency

        :param x:   the variable to read
        :return:    return the value that the operation read, False if the operation fails
        """
        assert self.status == Status.running
        # has some problems
        results = []
        for s in x.sites:
            if s.status == site.Status.running:
                ret, val = s.read(self, x, ts=self.creation_timestamp)
                if ret is True:
                    # print
                    logging.info(
                        'transaction %s reads %s=%d in its %d-th operation' % 
                        (self.name, x.name, val, self.next_op_index))
                    logging.info(val)
                    self.extra = '(site = %d, tick = %d)' % (
                        s.idx, self._tm.timestamp)
                    return val
        self.set_status(Status.ready)
        return False

    def write(self, x, val):
        # could not be called!
        assert False
