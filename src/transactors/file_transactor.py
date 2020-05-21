'''File Trnasactor'''

import logging
import multiprocessing
from time import sleep

from etl import ETL


class FileTransactor():
    '''File Transactor'''

    logger = logging.getLogger(__name__)

    count = 0
    queue = None

    def __init__(self):
        manager = multiprocessing.Manager()
        FileTransactor.queue = manager.Queue()
        self.filetracking_queue = manager.list()


    @staticmethod
    def _get_name():
        return "FileTransactor %s" % multiprocessing.current_process().name


    def start_threads(self, thread_count):
        '''Start Threads'''

        self.thread_pool = []
        for i in range(0, thread_count):
            process = multiprocessing.Process(target=self.run,
                                              name=str(i),
                                              args=(self.filetracking_queue,))
            process.start()
            self.thread_pool.append(process)


    @staticmethod
    def execute_transaction(sub_type):
        '''Execture Transaction'''

        FileTransactor.count = FileTransactor.count + 1
        FileTransactor.queue.put((sub_type, FileTransactor.count))
        FileTransactor.logger.debug("Execute Transaction Batch: %s QueueSize: %s ",
                                    FileTransactor.count,
                                    FileTransactor.queue.qsize())

    def check_for_thread_errors(self):
        '''Check for Thread Errors'''

        ETL.wait_for_threads(self.thread_pool, FileTransactor.queue)


    def wait_for_queues(self):
        '''Wait for Queues'''

        FileTransactor.queue.join()


    def shutdown(self):
        '''Shutdown'''

        self.logger.debug("Shutting down FileTransactor threads: %s",
                          len(self.thread_pool))
        for thread in self.thread_pool:
            thread.terminate()
        self.logger.debug("Finished Shutting down FileTransactor threads")

    def run(self, filetracking_queue):
        '''Run'''

        self.logger.debug("%s: Starting FileTransactor Thread Runner.", self._get_name())
        while True:
            try:
                (sub_type, FileTransactor.count) = FileTransactor.queue.get()
            except EOFError as error:
                self.logger.debug("Queue Closed exiting: %s", error)
                return

            self.logger.debug("%s: Pulled File Transaction Batch: %s QueueSize: %s ",
                              self._get_name(),
                              FileTransactor.count,
                              FileTransactor.queue.qsize())
            self.download_file(sub_type, filetracking_queue)
            FileTransactor.queue.task_done()
        #EOFError

    def download_file(self, sub_type, filetracking_queue):
        '''Download File'''

        filepath = sub_type.get_filepath()
        filepath_to_download = sub_type.get_file_to_download()

        self.logger.debug("%s: Acquiring file: %s from filepath: %s",
                          self._get_name(),
                          filepath,
                          filepath_to_download)

        self.logger.debug("%s: Checking whether the file is currently downloading: %s",
                          self._get_name(),
                          filepath_to_download)

        if filepath_to_download in filetracking_queue:
            self.logger.debug("%s: The file is already downloading, waiting for it to finish: %s",
                              self._get_name(),
                              filepath_to_download)
            while filepath_to_download in filetracking_queue:
                sleep(1)
            self.logger.debug("%s: File no longer downloading, proceeding: %s",
                              self._get_name(),
                              filepath_to_download)
            sub_type.get_data()
        else:
            self.logger.debug("%s: File not currently downloading, initiating download: %s",
                              self._get_name(),
                              filepath_to_download)
            filetracking_queue.append(filepath_to_download)
            sub_type.get_data()
            self.logger.debug("%s: Download complete. Removing item from download queue: %s",
                              self._get_name(),
                              filepath_to_download)
            filetracking_queue.remove(filepath_to_download)
