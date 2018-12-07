import logging
import multiprocessing
from time import sleep

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

    def wait_for_queues(self):
        FileTransactor.queue.join()
        
    def shutdown(self):       
        logger.info("Shutting down FileTransactor threads: %s" % len(self.thread_pool))
        for thread in self.thread_pool:
            thread.terminate()
        logger.info("Finished Shutting down FileTransactor threads")

    def run(self, filetracking_queue):
        logger.info("%s: Starting FileTransactor Thread Runner." % self._get_name())
        while True:
            try:
                ((sub_type, FileTransactor.count)) = FileTransactor.queue.get()
            except EOFError as error:
                logger.info("Queue Closed exiting: %s" % error)
                return
            logger.debug("%s: Pulled File Transaction Batch: %s QueueSize: %s " % (self._get_name(), FileTransactor.count, FileTransactor.queue.qsize()))  
            self.download_and_validate_file(sub_type, filetracking_queue)
            FileTransactor.queue.task_done()
        #EOFError

    def download_and_validate_file(self, sub_type, filetracking_queue):
        filepath = sub_type.get_filepath()
        filepath_to_download = sub_type.get_file_to_download()

        logger.info("%s: Checking whether file is already in the download queue: %s" % (self._get_name(), filepath_to_download))

        if filepath_to_download in filetracking_queue:
            logger.info("%s: File already exists in download queue: %s" % (self._get_name(), filepath_to_download))
            logger.info("%s: Waiting for file to exit download queue before proceeding: %s" % (self._get_name(), filepath_to_download))
            while filepath_to_download in filetracking_queue:
                sleep(1)
            logger.info("%s: File no longer found in download queue, proceeding: %s" % (self._get_name(), filepath_to_download))
            sub_type.get_data()
        else:
            logger.info("%s: File not found in download queue. Adding file: %s" % (self._get_name(), filepath_to_download))
            filetracking_queue.append(filepath_to_download)
            sleep(5)
            logger.info("%s: Getting data and downloading: %s" % (self._get_name(), filepath))
            sub_type.get_data()
            logger.info("%s: Download complete. Removing item from download queue: %s" % (self._get_name(), filepath_to_download))
            filetracking_queue.remove(filepath_to_download)

        logger.debug("%s: Downloading data finished. Starting validation: %s" % (self._get_name(), filepath))
        # sub_type.validate()
        logger.debug("%s: Validation finish: %s" % (self._get_name(), filepath))