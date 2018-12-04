import logging
logger = logging.getLogger(__name__)

from contextlib import ExitStack
import csv
from queue import Queue

from .transactor import Transactor
from .neo4j_transactor import Neo4jTransactor

class CSVTransactor(Transactor):

    count = 0
    queue = Queue(2000)

    def __init__(self):
        super().__init__()

    def _get_name(self):
        return "CSV %s" % self.threadid

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
            cypher_query_template = query_params.pop(0) # Remove the first item from the list.
            query_to_run = cypher_query_template % tuple(query_params) # Format the query with all remaining paramenters.
            while len(query_params) > 2: # We need to remove extra params before we append the modified query. Assuming the last entry in the list is the filepath
                query_params.pop() 
            query_params.append(query_to_run) # The final query is 3 elemnts: commit size, filename, and modified (complete) query.
        CSVTransactor.count = CSVTransactor.count + 1
        CSVTransactor.queue.put((generator, query_list_with_params, CSVTransactor.count))
        logger.debug("Execute Transaction Batch: %s QueueSize: %s " % (CSVTransactor.count, CSVTransactor.queue.qsize()))  

    def wait_for_queues(self):
        CSVTransactor.queue.join()

    def run(self):
        logger.info("%s: Starting CSVTransactor Thread Runner: " % self._get_name())
        while True:
            ((generator, query_list_with_params, CSVTransactor.count)) = CSVTransactor.queue.get()
            logger.debug("%s: Pulled CSV Transaction Batch: %s QueueSize: %s " % (self._get_name(), CSVTransactor.count, CSVTransactor.queue.qsize()))  
            self.save_file(generator, query_list_with_params)
            CSVTransactor.queue.task_done()

    def save_file(self, generator, query_list_with_params):

        with ExitStack() as stack:
            # Open all necessary CSV files at once.
            open_files = [stack.enter_context(open('tmp/' + query_params[1], 'w', encoding='utf-8')) for query_params in query_list_with_params]
            
            csv_file_writer = [None] * len(open_files) # Create a list with 'None' placeholder entries.

            for generator_entry in generator:
                for index, individual_list in enumerate(generator_entry):
                    current_filename = open_files[index].name # Our current CSV output file.

                    if len(individual_list) == 0:
                        logger.debug("%s: No data found when writing to csv! Skipping output file: %s" % (self._get_name(), current_filename))
                        continue

                    if csv_file_writer[index] is None: # If we haven't yet created a DictWriter for this particular file.
                        try:
                            csv_file_writer[index] = csv.DictWriter(open_files[index], fieldnames=list(individual_list[0]), quoting=csv.QUOTE_ALL)
                            csv_file_writer[index].writeheader() # Write the headers.
                        except Exception as e:
                            logger.critical("%s: Couldn't write to file: %s " % (self._get_name(), current_filename))
                            logger.critical(e)

                    csv_file_writer[index].writerows(individual_list) # Write the remainder of the list content for this iteration.
                    #logger.info("%s: Finished Writting %s entries to file: %s" % (self._get_name(), len(individual_list), current_filename))

        query_batch = []

        for query_param in query_list_with_params:
            query_batch.append([query_param[2], query_param[1]]) # neo4j query and filename.
        Neo4jTransactor.execute_query_batch(query_batch)