import logging
import os
from queue import Queue
import time

from neo4j import GraphDatabase

from transactors import Transactor


logger = logging.getLogger(__name__)

class Neo4jTransactor(Transactor):

    count = 0
    queue = Queue(2000)

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
        super().__init__()

    def _get_name(self):
        return "Neo4j %s" % self.threadid

    def start_threads(self, thread_count):
        thread_pool = []
        for n in range(0, thread_count):
            runner = Neo4jTransactor()
            runner.threadid = n
            runner.daemon = True
            runner.start()
            thread_pool.append(runner)

    #def run_single_query(self, query):
    #    with self.graph.session() as session:
    #        with session.begin_transaction() as tx:
    #            returnSet = tx.run(query)
    #    return returnSet     

    @staticmethod
    def execute_query_batch(query_batch):
        Neo4jTransactor.count = Neo4jTransactor.count + 1
        logger.info("Adding Query Batch: %s BatchSize: %s QueueSize: %s " % (Neo4jTransactor.count, len(query_batch), Neo4jTransactor.queue.qsize()))
        Neo4jTransactor.queue.put((query_batch, Neo4jTransactor.count))

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
        logger.info("%s: Starting Neo4jTransactor Thread Runner: " % self._get_name())
        while True:

            (query_batch, query_counter) = Neo4jTransactor.queue.get()
            logger.info("%s: Processing query batch: %s BatchSize: %s" % (self._get_name(), query_counter, len(query_batch)))
            batch_start = time.time()
            # Save VIA pickle rather then NEO
            #file_name = "tmp/transaction_%s" % batch_count
            #file = open(file_name,'wb')
            #logger.info("Writting to file: %s Records: %s" % (file_name, len(query_data)))
            #pickle.dump((cypher_query, query_data), file)
            #file.close() 

            for neo4j_query, filename in query_batch:
                logger.info("%s: Processing query for file: %s QueryNum: %s QueueSize: %s" % (self._get_name(), filename, query_counter, Neo4jTransactor.queue.qsize()))
                start = time.time()
                try:
                    session = Neo4jTransactor.graph.session()
                    session.run(neo4j_query)
                    session.close()
                    end = time.time()
                    elapsed_time = end - start
                    logger.info("%s: Processed query for file: %s QueryNum: %s QueueSize: %s Time: %s" % (self._get_name(), filename, query_counter, Neo4jTransactor.queue.qsize(), time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
                except Exception as e:
                    print(e)
                    logger.error("%s: Query Failed: %s" % (self._get_name(), neo4j_query))
                    #logger.warn("%s: Query Conflict, putting data back in rework Queue. Size: %s Batch#: %s" % (self.threadid, Neo4jTransactor.rework.qsize(), batch_count))
                    #Neo4jTransactor.queue.put((generator, filename, query, batch_count))

            batch_end = time.time()
            batch_elapsed_time = batch_end - batch_start
            logger.info("%s: Query Batch finished: %s BatchSize: %s Time: %s" % (self._get_name(), query_counter, len(query_batch), time.strftime("%H:%M:%S", time.gmtime(batch_elapsed_time))))
            Neo4jTransactor.queue.task_done()

