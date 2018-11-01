import time
import logging
import os
import pickle
from queue import Queue
from threading import Thread
from neo4j.v1 import GraphDatabase

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

    def execute_transaction(self, generator, filename, query):
        Neo4jTransactor.count = Neo4jTransactor.count + 1
        Neo4jTransactor.queue.put((generator, filename, query, Neo4jTransactor.count))
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

    def wait_for_queues():
        Neo4jTransactor.rework.join()
        Neo4jTransactor.queue.join()

    def run(self):
        logger.info("%s: Starting Neo4jTransactor Thread Runner: " % self.threadid)
        last_tx = time.time()
        while True:

            (generator, filename, query, query_count) = Neo4jTransactor.queue.get()
            
            self.save_file(generator, filename)
            start = time.time()
            
            # Save VIA pickle rather then NEO
            #file_name = "tmp/transaction_%s" % batch_count
            #file = open(file_name,'wb')
            #logger.info("Writting to file: %s Records: %s" % (file_name, len(query_data)))
            #pickle.dump((cypher_query, query_data), file)
            #file.close() 
            
            try:
                session = Neo4jTransactor.graph.session()
                tx = session.begin_transaction()
                tx.run(cypher_query)
                tx.commit()
                session.close()
                end = time.time()

                logger.info("%s: Processed query. QueueSize: %s BatchNum: %s" % (self.threadid, Neo4jTransactor.queue.qsize(), query_count))
            except:
                logger.error("%s: Query Failed: %s" % (self.threadid, query))
                #logger.warn("%s: Query Conflict, putting data back in rework Queue. Size: %s Batch#: %s" % (self.threadid, Neo4jTransactor.rework.qsize(), batch_count))
                #Neo4jTransactor.queue.put((generator, filename, query, batch_count))

            Neo4jTransactor.queue.task_done()

    def save_file(self, data_generator, filename):
        logger.info("Writing data to CSV: %s" % filename)
        with open(filename, mode='w') as csv_file:
            first_entry = next(data_generator) # Used for obtaining the keys in the dictionary.
            csv_file_writer = csv.DictWriter(csv_file, fieldnames=list(first_entry[0]), quoting=csv.QUOTE_ALL)
            csv_file_writer.writeheader() # Write the header.
            csv_file_writer.writerows(first_entry) # Write the first entry from earlier.
            for entries in data_generator:
                csv_file_writer.writerows(entries)
        logger.info("Finished writting data to CSV: %s" % filename)
