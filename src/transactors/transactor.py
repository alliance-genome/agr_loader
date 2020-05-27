"""Transactor"""

from threading import Thread


class Transactor(Thread):
    """Transactor"""

    def __init__(self):
        Thread.__init__(self)
        self.threadid = 0
