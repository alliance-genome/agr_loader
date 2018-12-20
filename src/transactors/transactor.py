import logging
from threading import Thread

logger = logging.getLogger(__name__)


class Transactor(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.threadid = 0