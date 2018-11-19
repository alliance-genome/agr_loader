import logging
logger = logging.getLogger(__name__)

from contextlib import ExitStack
import csv, os, logging, sys, logging
from queue import Queue
from files import S3File, TXTFile, TARFile, Download 
import threading

from .transactor import Transactor

class FileTransactor(object):

    count = 0
    queue = Queue(2000)

    def __init__(self):
        pass

    def start_threads(self, thread_count):
        thread_pool = []
        for n in range(0, thread_count):
            runner = threading.Thread(target=self.run)
            runner.threadid = n
            runner.daemon = True
            runner.start()
            thread_pool.append(runner)

    @staticmethod
    def execute_transaction(sub_type):
        FileTransactor.count = FileTransactor.count + 1
        FileTransactor.queue.put((sub_type, FileTransactor.count))
        logger.info("Execute Transaction Batch: %s QueueSize: %s " % (FileTransactor.count, FileTransactor.queue.qsize()))  

    def wait_for_queues(self):
        FileTransactor.queue.join()

    def run(self):
        logger.info("%s: Starting FileTransactor Thread Runner." % threading.currentThread().getName())
        while True:
            ((sub_type, FileTransactor.count)) = FileTransactor.queue.get()
            logger.info("%s: Pulled File Transaction Batch: %s QueueSize: %s " % (threading.currentThread().getName(), FileTransactor.count, FileTransactor.queue.qsize()))  
            self.download_and_validate_file(sub_type)
            FileTransactor.queue.task_done()

    def download_and_validate_file(self, sub_type):

        logger.info("%s: Getting data and downloading: %s" % (threading.currentThread().getName(), sub_type.get_filepath()))
        sub_type.get_data()
        logger.info("%s: Downloading data finished. Starting validation: %s" % (threading.currentThread().getName(), sub_type.get_filepath()))
        # sub_type.validate()
        logger.info("%s: Validation finish: %s" % (threading.currentThread().getName(), sub_type.get_filepath()))