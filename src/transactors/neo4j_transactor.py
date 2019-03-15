import logging
import multiprocessing
import os
import pickle
import time
from etl import *

from neo4j.v1 import GraphDatabase


logger = logging.getLogger(__name__)


class Neo4jTransactor(object):

    count = 0
    queue = None
    
    if "USING_PICKLE" in os.environ and os.environ['USING_PICKLE'] == "True":
        using_pickle = True
    else:
        using_pickle = False

    def __init__(self):
        pass

    def _get_name(self):
        return "Neo4jTransactor %s" % multiprocessing.current_process().name

    def start_threads(self, thread_count):
        m = multiprocessing.Manager()
        q = m.Queue()
        Neo4jTransactor.queue = q
        self.thread_pool = []
        for i in range(0, thread_count):
            p = multiprocessing.Process(target=self.run, name=str(i))
            p.start()
            self.thread_pool.append(p)

    def shutdown(self):
        logger.info("Shutting down Neo4jTransactor threads: %s" % len(self.thread_pool))
        for thread in self.thread_pool:
            thread.terminate()
        logger.info("Finished Shutting down Neo4jTransactor threads")

    @staticmethod
    def execute_query_batch(query_batch):
        Neo4jTransactor.count = Neo4jTransactor.count + 1
        logger.debug("Adding Query Batch: %s BatchSize: %s QueueSize: %s " % (Neo4jTransactor.count, len(query_batch), Neo4jTransactor.queue.qsize()))
        Neo4jTransactor.queue.put((query_batch, Neo4jTransactor.count))

    def wait_for_queues(self):
        ETL.wait_for_threads(self.thread_pool, Neo4jTransactor.queue)

    def run(self):
        
        if "NEO4J_NQC_HOST" in os.environ:
            host = os.environ['NEO4J_NQC_HOST']
        else:
            host = "localhost"
            
        if "NEO4J_NQC_PORT" in os.environ:
            port = int(os.environ['NEO4J_NQC_PORT'])
        else:
            port = 7687
    
        if Neo4jTransactor.using_pickle is False:
            uri = "bolt://" + host + ":" + str(port)
            graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1)
        
        logger.info("%s: Starting Neo4jTransactor Thread Runner: " % self._get_name())
        while True:
            try:
                (query_batch, query_counter) = Neo4jTransactor.queue.get()
            except EOFError as error:
                logger.info("Queue Closed exiting: %s" % error)
                return
            
            logger.debug("%s: Processing query batch: %s BatchSize: %s" % (self._get_name(), query_counter, len(query_batch)))
            batch_start = time.time()

            total_query_counter = 0

            while len(query_batch) > 0:

                (neo4j_query, filename) = query_batch.pop(0)
                
                logger.debug("%s: Processing query for file: %s QueryNum: %s QueueSize: %s" % (self._get_name(), filename, query_counter, Neo4jTransactor.queue.qsize()))
                start = time.time()
                try:
                    
                    if Neo4jTransactor.using_pickle is True:
                        # Save VIA pickle rather then NEO
                        file_name = "tmp/temp/transaction_%s_%s" % (query_counter, total_query_counter)
                        file = open(file_name,'wb')
                        logger.debug("Writting to file: tmp/temp/transaction_%s_%s" % (query_counter, total_query_counter))
                        pickle.dump(neo4j_query, file)
                        file.close()
                    else:
                        session = graph.session()
                        session.run(neo4j_query)
                        session.close()
                    
                    end = time.time()
                    elapsed_time = end - start
                    logger.info("%s: Processed query for file: %s QueryNum: %s QueueSize: %s Time: %s" % (self._get_name(), filename, query_counter, Neo4jTransactor.queue.qsize(), time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
                except Exception as e:
                    logger.error(e)
                    #logger.error("%s: Query Failed: %s" % (self._get_name(), neo4j_query))
                    # TODO Extract and print NODE information from error message. Would be helpful for troubleshooting.
                    logger.warn("%s: Query Conflict, putting data back in Queue to run later. %s" % (self._get_name(), filename))
                    query_batch.insert(0, (neo4j_query, filename))
                    time.sleep(12)
                    Neo4jTransactor.queue.put((query_batch, query_counter))
                    break
                
                total_query_counter = total_query_counter + 1

            batch_end = time.time()
            batch_elapsed_time = batch_end - batch_start
            logger.debug("%s: Query Batch finished: %s BatchSize: %s Time: %s" % (self._get_name(), query_counter, len(query_batch), time.strftime("%H:%M:%S", time.gmtime(batch_elapsed_time))))
            Neo4jTransactor.queue.task_done()
