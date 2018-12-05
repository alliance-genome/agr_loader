import logging
import multiprocessing

logger = logging.getLogger(__name__)

class FileTransactor(object):

    count = 0
    queue = None

    def __init__(self):
        m = multiprocessing.Manager()
        FileTransactor.queue = m.Queue()
    
    def _get_name(self):
        return "FileTransactor %s" % multiprocessing.current_process().name

    def start_threads(self, thread_count):
        self.thread_pool = []
        for i in range(0, thread_count):
            p = multiprocessing.Process(target=self.run, name=str(i))
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

    def run(self):
        logger.info("%s: Starting FileTransactor Thread Runner." % self._get_name())
        while True:
            try:
                ((sub_type, FileTransactor.count)) = FileTransactor.queue.get()
            except EOFError as error:
                logger.info("Queue Closed exiting: %s" % error)
                return
            logger.debug("%s: Pulled File Transaction Batch: %s QueueSize: %s " % (self._get_name(), FileTransactor.count, FileTransactor.queue.qsize()))  
            self.download_and_validate_file(sub_type)
            FileTransactor.queue.task_done()
        #EOFError

    def download_and_validate_file(self, sub_type):

        logger.info("%s: Getting data and downloading: %s" % (self._get_name(), sub_type.get_filepath()))
        sub_type.get_data()
        logger.debug("%s: Downloading data finished. Starting validation: %s" % (self._get_name(), sub_type.get_filepath()))
        # sub_type.validate()
        logger.debug("%s: Validation finish: %s" % (self._get_name(), sub_type.get_filepath()))