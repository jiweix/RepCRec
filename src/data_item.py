# -----------------------------------------------------------------------------
# dataitem.py
#
# Classes for data items
# -----------------------------------------------------------------------------

import time
import re


class DataItem(object):
    """
    DataItem is used to indicate the 20 variables.
    Odd indexed variables are stored at site (1+index)%10, Even indexed ones are stored at all sites.
    All data items should be initialized in adb.py
    """
    def __init__(self, tm, name):
        """
        Create one data item
        Maintain a list of all sites where this data item reside

        :param tm:      the global transaction manager
        :param name:    string in the format of x1 ... x20
        """
        match = re.match(r'x(\d+)$', name)
        assert match is not None
        num = int(match.group(1))
        site_ids = [num % 10] if num % 2 == 1 else range(10)
        self.name = name
        self.sites = map(lambda i: tm.sites[i], site_ids)
        # initialization
        map(lambda s: s.write(None, self, num * 10), self.sites)
        map(lambda s: s.commit(None), self.sites)
