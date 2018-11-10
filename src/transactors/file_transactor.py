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
        return "Filename %s" % self.threadid

    def start_threads(self, thread_count):
        thread_pool = []
        for n in range(0, thread_count):
            runner = FileTransactor()
            runner.threadid = n
            runner.daemon = True
            runner.start()
            thread_pool.append(runner)

    @staticmethod
    def execute_transaction(downloadable_item):
        FileTransactor.queue.put((downloadable_item, FileTransactor.count))
        logger.info("Execute Transaction Batch: %s QueueSize: %s " % (FileTransactor.count, FileTransactor.queue.qsize()))  

    def wait_for_queues(self):
        FileTransactor.queue.join()

    def run(self):
        logger.info("%s: Starting FileTransactor Thread Runner: " % self._get_name())
        while True:
            ((downloadable_item, FileTransactor.count)) = FileTransactor.queue.get()
            logger.info("%s: Pulled File Transaction Batch: %s QueueSize: %s " % (self._get_name(), FileTransactor.count, FileTransactor.queue.qsize()))  
            self.download_and_validate_file(downloadable_item)
            FileTransactor.queue.task_done()

    def download_and_validate_file(self, downloadable_item):

        # Grab the data (TODO validate).
        # Some of this algorithm is temporary.
        # e.g. Files from the submission system will arrive without the need for unzipping, etc.

        path = 'tmp'

        if downloadable_item[1] is not None:
            if downloadable_item[1].startswith('http'):
                download_filename = os.path.basename(downloadable_item[2])
                download_object = Download(path, downloadable_item[1], download_filename) # savepath, urlToRetieve, filenameToSave
                download_object.get_downloaded_data()
            else:
                S3File(downloadable_item[1], path).download()
                if downloadable_item[1].endswith('tar.gz'):
                    tar_object = TARFile(path, downloadable_item[1])
                    tar_object.extract_all()
        else: 
            logger.warn('No download path specified, assuming download is not required.')
            
        # with ExitStack() as stack:
        #     # Open all necessary CSV files at once.
        #     open_files = [stack.enter_context(open('tmp/' + fname[2], 'w', encoding='utf-8')) for fname in query_list_with_params]

        #     first_entry = next(generator)

        #     for generator_entry in generator:
        #         for index, data_entries in enumerate(generator_entry):
        #             filename = query_list_with_params[index][2]
        #             try:
        #                 csv_file_writer = csv.DictWriter(open_files[index], fieldnames=list(first_entry[index][0]), quoting=csv.QUOTE_ALL)
        #             except IndexError:
        #                 logger.warn("Saving Data Failed: File is empty: %s" % (filename))
        #                 continue

        #             csv_file_writer.writeheader()
        #             csv_file_writer.writerows(first_entry[index])
        #             for index2, entry_list in enumerate(generator_entry):
        #                 csv_file_writer.writerows(entry_list)

        # with ExitStack() as stack:
        #     # Open all necessary CSV files at once.
        #     open_files = [stack.enter_context(open('tmp/' + fname[2], 'w', encoding='utf-8')) for fname in query_list_with_params]

        #     # Grab the first set of lists from the generator.
        #     # The are stored in a tuple.
        #     first_entry = next(generator)

        #     # Create the csv_writer for each file. Write the header and the first batch of data.
        #     csv_writer_list = []
        #     for index, open_file in enumerate(open_files):
        #         filename = query_list_with_params[index][2]
        #         try:
        #             logger.info("Saving Data in file: %s" % filename)
        #             csv_file_writer = csv.DictWriter(open_file, fieldnames=list(first_entry[index][0]), quoting=csv.QUOTE_ALL)
        #         except IndexError:
        #             logger.warn("Saving Data Failed: File is empty: %s" % (filename))
        #             continue
        #         csv_file_writer.writeheader()
        #         csv_file_writer.writerows(first_entry[index])
        #         csv_writer_list.append(csv_file_writer)
        #         logger.warn("Saving Data Complete: %s" % filename)
            
        #     # Write the rest of the data per file for each list in the generator.
        #     for generator_entry in generator:
        #         for index, individual_list in enumerate(generator_entry):
        #             csv_writer_list[index].writerows(individual_list)
