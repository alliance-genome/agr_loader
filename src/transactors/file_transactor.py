import logging
import multiprocessing
from time import sleep

from etl import ETL

logger = logging.getLogger(__name__)


class FileTransactor(object):

    count = 0
    queue = None

    def __init__(self):
        m = multiprocessing.Manager()
        FileTransactor.queue = m.Queue()
        self.filetracking_queue = m.list()
    
    def _get_name(self):
        return "FileTransactor %s" % multiprocessing.current_process().name

    def start_threads(self, thread_count):
        self.thread_pool = []
        for i in range(0, thread_count):
            p = multiprocessing.Process(target=self.run, name=str(i), args=(self.filetracking_queue,))
            p.start()
            self.thread_pool.append(p)

    @staticmethod
    def execute_transaction(sub_type):
        FileTransactor.count = FileTransactor.count + 1
        FileTransactor.queue.put((sub_type, FileTransactor.count))
        logger.debug("Execute Transaction Batch: %s QueueSize: %s " % (FileTransactor.count, FileTransactor.queue.qsize()))  

    def check_for_thread_errors(self):
        ETL.wait_for_threads(self.thread_pool, FileTransactor.queue)

    def wait_for_queues(self):
        FileTransactor.queue.join()
        
    def shutdown(self):       
        logger.debug("Shutting down FileTransactor threads: %s" % len(self.thread_pool))
        for thread in self.thread_pool:
            thread.terminate()
        logger.debug("Finished Shutting down FileTransactor threads")

    def run(self, filetracking_queue):
        logger.debug("%s: Starting FileTransactor Thread Runner." % self._get_name())
        while True:
            try:
                (sub_type, FileTransactor.count) = FileTransactor.queue.get()
            except EOFError as error:
                logger.debug("Queue Closed exiting: %s" % error)
                return
            logger.debug("%s: Pulled File Transaction Batch: %s QueueSize: %s " % (self._get_name(), FileTransactor.count, FileTransactor.queue.qsize()))  
            self.download_file(sub_type, filetracking_queue)
            FileTransactor.queue.task_done()
        #EOFError

    def download_file(self, sub_type, filetracking_queue):
        filepath = sub_type.get_filepath()
        filepath_to_download = sub_type.get_file_to_download()

        logger.debug("%s: Acquiring file: %s from filepath: %s" % (self._get_name(), filepath, filepath_to_download))

        logger.debug("%s: Checking whether the file is currently downloading: %s" % (self._get_name(), filepath_to_download))

        if filepath_to_download in filetracking_queue:
            logger.debug("%s: The file is already downloading, waiting for it to finish: %s" % (self._get_name(), filepath_to_download))
            while filepath_to_download in filetracking_queue:
                sleep(1)
            logger.debug("%s: File no longer downloading, proceeding: %s" % (self._get_name(), filepath_to_download))
            sub_type.get_data()
        else:
            logger.debug("%s: File not currently downloading, initiating download: %s" % (self._get_name(), filepath_to_download))
            filetracking_queue.append(filepath_to_download)
            sub_type.get_data()
            logger.debug("%s: Download complete. Removing item from download queue: %s" % (self._get_name(), filepath_to_download))
            filetracking_queue.remove(filepath_to_download)
