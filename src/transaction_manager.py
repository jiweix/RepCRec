# -----------------------------------------------------------------------------
# transcation_manager.py
#
# Classes for transaction manager
# -----------------------------------------------------------------------------

from transaction import Status as TransactionStatus
import site1 as site


class TransactionManager(object):
    """
    Transaction manager manages all transactions, performs operations as requested by transactions,
    detects deadlocks and resolves the problem by killing the youngest transaction.
    """
    def __init__(self):
        """
        Create transaction list, system timestamp, operation id, and list of sites
        """
        self.transactions = list()
        self.timestamp = 0
        self.sites = [site.Site(self, i + 1) for i in xrange(10)]
        self._op_id = 0

    def sleep(self, timeout=1):
        self.timestamp += timeout

    def next_tick(self):
        """
        Let all ready transactions run next operation
        operation id is used for FIFO
        Detect deadlock after try to run all operation
        """
        ready_transactions = filter(
            lambda t: t.status == TransactionStatus.ready,
            self.transactions)
        map(lambda t: t.set_status(TransactionStatus.running),
            ready_transactions)
        running_transactions = filter(
            lambda t: t.status == TransactionStatus.running and t.next_op is not None,
            self.transactions)
        blocked_transactions = filter(
            lambda t: t.status == TransactionStatus.blocked,
            self.transactions)
        running_transactions.sort(key=lambda t: t.next_op.id)
        for t in running_transactions:
            t.next_operation()
        # if blocked => ready: run it
        waked_transactions = filter(
            lambda t: t.status == TransactionStatus.ready,
            blocked_transactions)
        waked_transactions.sort(key=lambda t: t.next_op.id)
        map(lambda t: t.set_status(TransactionStatus.running),
            waked_transactions)
        for t in waked_transactions:
            t.next_operation()

        created_transactions = filter(
            lambda t: t.status == TransactionStatus.created,
            self.transactions)
        map(lambda t: t.set_status(TransactionStatus.ready),
            created_transactions)
        self.detect_deadlocks()

    def detect_deadlocks(self):
        """
        SCC is used to detect cycles from all blocked transactions.
        Youngest Transaction in a SCC is scheduled to be killed
        The while loop will end until there is no SCC with size larger than one exists in blocked transactions.
        """
        blocked_transactions = filter(
            lambda t: t.status == TransactionStatus.blocked,
            self.transactions)
        if len(blocked_transactions) <= 1:
            return
        SCCs = self._get_SCCs(blocked_transactions)
        to_kill = list()
        while self._check_SCCs(SCCs):
            for SCC in SCCs:
                if len(SCC) >= 2:
                    SCC.sort(key=lambda t: t.creation_timestamp, reverse=True)
                    to_kill.append(SCC[0])
                    blocked_transactions = [t for t in blocked_transactions if t != SCC[0]]
            SCCs = self._get_SCCs(blocked_transactions)
        # kill youngest
        to_kill.sort(key=lambda t: t.creation_timestamp, reverse=True)
        for t in to_kill:
            t.kill()

    def new_transaction(self, t):
        self.transactions.append(t)

    def get_op_id(self):
        """
        :return: the operation id assigned by TM
        """
        self._op_id += 1
        return self._op_id

    def _check_SCCs(self, sccs):
        for scc in sccs:
            if len(scc) >= 2:
                return True
        return False

    def _get_SCCs(self, transactions):
        stack = []
        visited = {}
        for t in transactions:
            visited[t] = False
        for t in transactions:
            if not visited[t]:
                self._fillOrder(transactions, t, visited, stack)
        # Second DFS
        for t in transactions:
            visited[t] = False
        res = list()
        SCC = list()
        while stack:
            t = stack.pop()
            if not visited[t]:
                self._second_DFS(transactions, t, visited, SCC)
                res.append(SCC)
                SCC = list()
        return res

    def _second_DFS(self, transactions, v, visited, SCC):
        visited[v] = True
        SCC.append(v)
        for t in v.waited_by:
            if t in transactions:
                if not visited[t]:
                    self._second_DFS(transactions, t, visited, SCC)

    def _fillOrder(self, transactions, v, visited, stack):
        visited[v] = True
        for t in v.wait_for:
            if t in transactions:
                if not visited[t]:
                    self._fillOrder(transactions, t, visited, stack)
        stack = stack.append(v)
