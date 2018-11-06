import time
import logging
import os
import csv, sys
from queue import Queue
from contextlib import ExitStack
from transactors import *
from .neo4j_transactor import Neo4jTransactor

logger = logging.getLogger(__name__)

class CSVTransactor(Transactor):

    count = 0
    queue = Queue(2000)

    def __init__(self):
        super().__init__()

    def start_threads(self, thread_count):
        thread_pool = []
        for n in range(0, thread_count):
            runner = CSVTransactor()
            runner.threadid = n
            runner.daemon = True
            runner.start()
            thread_pool.append(runner)

    @staticmethod
    def execute_transaction(generator, query_list_with_params):
        for query_params in query_list_with_params:
            query_to_run = query_params[0] % (query_params[1], query_params[2])
            query_params.append(query_to_run) # items [3] will be the final query to run
        CSVTransactor.count = CSVTransactor.count + 1
        CSVTransactor.queue.put((generator, query_list_with_params, CSVTransactor.count))
        logger.info("Execute Transaction Batch: %s QueueSize: %s " % (CSVTransactor.count, CSVTransactor.queue.qsize()))  

    def wait_for_queues(self):
        CSVTransactor.queue.join()

    def run(self):
        logger.info("%s: Starting CSVTransactor Thread Runner: " % self.threadid)
        last_tx = time.time()
        while True:
            ((generator, query_list_with_params, CSVTransactor.count)) = CSVTransactor.queue.get()
            logger.info("%s: Pulled CSV Transaction Batch: %s QueueSize: %s " % (self.threadid, CSVTransactor.count, CSVTransactor.queue.qsize()))  
            self.save_file(generator, query_list_with_params)
            CSVTransactor.queue.task_done()


    def save_file(self, generator, query_list_with_params):

        csv_file_writer_list = []

        with ExitStack() as stack:
            # Open all necessary CSV files at once.
            open_files = [stack.enter_context(open('tmp/' + query_params[2], 'w', encoding='utf-8')) for query_params in query_list_with_params]

            for generator_run_number, generator_entry in enumerate(generator):
                for index, individual_list in enumerate(generator_entry):
                    #logger.info("%s: Saving: %s entries to file." % (self.threadid, len(individual_list)))
                    if len(individual_list) == 0:
                        logger.warn("No data found when writing to %s! Skipping output file." % (open_files[index].name))
                        if generator_run_number == 0:
                            # On the first pass, add a "None" csv_writer to the file_writer list.
                            csv_file_writer_list.append(None)
                        continue
                    if generator_run_number == 0:
                        # Attempt to write the headers from the keys of the first dictionary entry.
                        try:
                            csv_file_writer = csv.DictWriter(open_files[index], fieldnames=list(individual_list[0]), quoting=csv.QUOTE_ALL)
                            csv_file_writer.writeheader()
                        except Exception as e:
                            logger.critical("Couldn't write to file: %s " % (open_files[index].name))
                            logger.critical(e.args)
                        csv_file_writer_list.append(csv_file_writer)
                    # Write the remaining contents of the generator-derived list to the file.
                    try:
                        csv_file_writer_list[index].writerows(individual_list)
                        logger.info("%s: Finished writing %s entries to file: %s" % (self.threadid, len(individual_list), open_files[index].name))
                    except Exception as e:
                        logger.critical("Couldn't write to file: %s " % (open_files[index].name))
                        logger.critical(e.args)
                    
        for query_param in query_list_with_params:
            Neo4jTransactor.execute_transaction(query_param[3])
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
