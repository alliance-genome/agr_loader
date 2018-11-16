from contextlib import ExitStack
import csv, os, logging, sys, logging
import logging
from queue import Queue
from files import S3File, TXTFile, TARFile, Download 

from .transactor import Transactor

logger = logging.getLogger(__name__)

class FileTransactor(Transactor):

    count = 0
    queue = Queue(2000)

    def __init__(self):
        super().__init__()

    def _get_name(self):
        return "FILE THREAD %s" % self.threadid

    def start_threads(self, thread_count):
        thread_pool = []
        for n in range(0, thread_count):
            runner = FileTransactor()
            runner.threadid = n
            runner.daemon = True
            runner.start()
            thread_pool.append(runner)

    @staticmethod
    def execute_transaction(sub_type):
        FileTransactor.queue.put((sub_type, FileTransactor.count))
        logger.info("Execute Transaction Batch: %s QueueSize: %s " % (FileTransactor.count, FileTransactor.queue.qsize()))  

    def wait_for_queues(self):
        FileTransactor.queue.join()

    def run(self):
        logger.info("%s: Starting FileTransactor Thread Runner: " % self._get_name())
        while True:
            ((sub_type, FileTransactor.count)) = FileTransactor.queue.get()
            logger.info("%s: Pulled File Transaction Batch: %s QueueSize: %s " % (self._get_name(), FileTransactor.count, FileTransactor.queue.qsize()))  
            self.download_and_validate_file(sub_type)
            FileTransactor.queue.task_done()

    def download_and_validate_file(self, sub_type):

        logger.info("%s: Processing file: %s" % (self._get_name(), sub_type.get_filepath()))
        sub_type.get_data()
        logger.info("%s: Received file %s starting validation." % (self._get_name(), sub_type.get_filepath()))
        sub_type.validate()
        logger.info("%s: Validation for %s finished." % (self._get_name(), sub_type.get_filepath()))
