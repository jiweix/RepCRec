
import transaction
import lock
from collections import namedtuple


def test1():
	TM = namedtuple('TM', ['timestamp'])
	tm = TM(1)
	t1 = transaction.ReadWriteTransaction(tm, 't1', transaction.Status.running)
	t2 = transaction.ReadWriteTransaction(tm, 't2', transaction.Status.running)
	t3 = transaction.ReadWriteTransaction(tm, 't3', transaction.Status.running)
	t4 = transaction.ReadWriteTransaction(tm, 't4', transaction.Status.running)
	t5 = transaction.ReadWriteTransaction(tm, 't5', transaction.Status.running)
	lk = lock.FIFOLock()

	assert lk.acquire(t1, lock.Mode.read)
	print map(lambda x: x.name, list(lk.holders))
	assert lk.acquire(t2, lock.Mode.read)
	print map(lambda x: x.name, list(lk.holders))
	assert lk.acquire(t3, lock.Mode.read)
	print map(lambda x: x.name, list(lk.holders))
	assert lk.acquire(t4, lock.Mode.write) is not True
	print map(lambda x: x.name, list(lk.holders))
	assert lk.acquire(t5, lock.Mode.read) is not True
	print map(lambda x: x.name, list(lk.holders))
	assert lk.acquire(t1, lock.Mode.write) is not True
	print map(lambda x: x.name, list(lk.holders))
	assert lk.acquire(t1, lock.Mode.read)
	print map(lambda x: x.name, list(lk.holders))
	assert lk.acquire(t2, lock.Mode.read)
	print map(lambda x: x.name, list(lk.holders))
	assert lk.acquire(t3, lock.Mode.read)
	print map(lambda x: x.name, list(lk.holders))
	assert lk.acquire(t1, lock.Mode.write) is not True
	lk.release(t2)
	assert lk.acquire(t1, lock.Mode.read)
	assert lk.acquire(t3, lock.Mode.read)
	assert lk.acquire(t1, lock.Mode.write) is not True
	lk.release(t3)
	assert lk.acquire(t1, lock.Mode.read)
	assert lk.acquire(t1, lock.Mode.write)
	lk.release(t1)
	assert lk.acquire(t4, lock.Mode.write)
	lk.release(t4)
	assert lk.acquire(t5, lock.Mode.read)


def test2():
	TM = namedtuple('TM', ['timestamp'])
        tm = TM(1)
        t1 = transaction.ReadWriteTransaction(tm, 't1', transaction.Status.running)
        t2 = transaction.ReadWriteTransaction(tm, 't2', transaction.Status.running)
	t3 = transaction.ReadWriteTransaction(tm, 't3', transaction.Status.running)
	lk = lock.FIFOLock()

	assert lk.acquire(t1, lock.Mode.read)
	assert lk.acquire(t2, lock.Mode.write) is not True
	assert lk.acquire(t3, lock.Mode.read) is not True
	t2.status = transaction.Status.aborted
	assert lk.acquire(t3, lock.Mode.read)

