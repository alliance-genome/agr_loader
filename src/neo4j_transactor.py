import time
import logging
import os
import csv
import pickle
from queue import Queue
from threading import Thread
from neo4j.v1 import GraphDatabase
from contextlib import ExitStack

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Neo4jTransactor(Thread):

    count = 0
    queue = Queue(20)

    if "NEO4J_NQC_HOST" in os.environ:
        host = os.environ['NEO4J_NQC_HOST']
    else:
        host = "localhost"
        
    if "NEO4J_NQC_PORT" in os.environ:
        port = int(os.environ['NEO4J_NQC_PORT'])
    else:
        port = 7687

    uri = "bolt://" + host + ":" + str(port)
    graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1)

    def __init__(self):
        Thread.__init__(self)
        self.threadid = 0

    #def run_single_query(self, query):
    #    with self.graph.session() as session:
    #        with session.begin_transaction() as tx:
    #            returnSet = tx.run(query)
    #    return returnSet     

    @staticmethod
    def execute_transaction(generator, data_tuple_list):
        Neo4jTransactor.count = Neo4jTransactor.count + 1
        Neo4jTransactor.queue.put((generator, data_tuple_list, Neo4jTransactor.count))
        logger.info("Adding Items Batch: %s QueueSize: %s " % (Neo4jTransactor.count, Neo4jTransactor.queue.qsize()))         

    #def run_single_parameter_query(self, query, parameter):
    #    logger.debug("Running run_single_parameter_query. Please wait...")
    #    logger.debug("Query: %s" % query)
    #    with Neo4jTransactor.graph.session() as session:
    #        with session.begin_transaction() as tx:
    #            returnSet = tx.run(query, parameter=parameter)
    #    return returnSet

    #def execute_transaction_batch(self, query, data, batch_size):
    #    logger.info("Executing batch query. Please wait...")
    #    logger.debug("Query: " + query)
    #    for submission in self.split_into_chunks(data, batch_size):
    #        self.execute_transaction(query, submission)
    #    logger.info("Finished batch loading.")

    #def split_into_chunks(self, data, batch_size):
    #    return (data[pos:pos + batch_size] for pos in range(0, len(data), batch_size))

    def wait_for_queues(self):
        Neo4jTransactor.queue.join()

    def run(self):
        logger.info("%s: Starting Neo4jTransactor Thread Runner: " % self.threadid)
        last_tx = time.time()
        while True:

            ((generator, data_tuple_list, Neo4jTransactor.count)) = Neo4jTransactor.queue.get()
            
            list_of_queries = []
            for possible_queries in data_tuple_list:
                # Add the filename for each file to the appropriate query.
                query_to_run = possible_queries[1] % possible_queries[0]
                # Append the query once the filename is specified.
                list_of_queries.append(query_to_run)

            start = time.time()
            self.save_file(generator, data_tuple_list)

            # Save VIA pickle rather then NEO
            #file_name = "tmp/transaction_%s" % batch_count
            #file = open(file_name,'wb')
            #logger.info("Writting to file: %s Records: %s" % (file_name, len(query_data)))
            #pickle.dump((cypher_query, query_data), file)
            #file.close() 
            
            for cypher_query in list_of_queries:
                try:
                    session = Neo4jTransactor.graph.session()
                    session.run(cypher_query)
                    session.close()
                    end = time.time()
                    logger.info("%s: Processed query. QueueSize: %s" % (self.threadid, Neo4jTransactor.queue.qsize))
                except Exception as e:
                    print(e)
                    logger.error("%s: Query Failed: %s" % (self.threadid, cypher_query))
                    #logger.warn("%s: Query Conflict, putting data back in rework Queue. Size: %s Batch#: %s" % (self.threadid, Neo4jTransactor.rework.qsize(), batch_count))
                    #Neo4jTransactor.queue.put((generator, filename, query, batch_count))

            Neo4jTransactor.queue.task_done()

    def save_file(self, generator, data_tuple_list):
        with ExitStack() as stack:
            # Open all necessary CSV files at once.
            open_files = [stack.enter_context(open('tmp/' + fname[0], 'w', encoding='utf-8')) for fname in data_tuple_list]

            # Grab the first set of lists from the generator.
            # The are stored in a tuple.
            first_entry = next(generator)              

            # Create the csv_writer for each file. Write the header and the first batch of data.
            csv_writer_list = []
            for index, open_file in enumerate(open_files):
                try: 
                    csv_file_writer = csv.DictWriter(open_file, fieldnames=list(first_entry[index][0]), quoting=csv.QUOTE_ALL)
                except IndexError:
                    print(first_entry[index])
                    print(first_entry[index][0])
                csv_file_writer.writeheader()
                csv_file_writer.writerows(first_entry[index])
                csv_writer_list.append(csv_file_writer)
            
            # Write the rest of the data per file for each list in the generator.
            for generator_entry in generator:
                for index, individual_list in enumerate(generator_entry):
                    csv_writer_list[index].writerows(individual_list)       
