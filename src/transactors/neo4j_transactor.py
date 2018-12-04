import logging

import os
import pickle
from queue import Queue
from etl.helpers import Neo4jHelper
import time

from transactors import Transactor

logger = logging.getLogger(__name__)

class Neo4jTransactor(Transactor):

    count = 0
    queue = Queue(2000)
    
    if "USING_PICKLE" in os.environ and os.environ['USING_PICKLE'] == "True":
        using_pickle = True
    else:
        using_pickle = False

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

    @staticmethod
    def execute_query_batch(query_batch):
        Neo4jTransactor.count = Neo4jTransactor.count + 1
        logger.info("Adding Query Batch: %s BatchSize: %s QueueSize: %s " % (Neo4jTransactor.count, len(query_batch), Neo4jTransactor.queue.qsize()))
        Neo4jTransactor.queue.put((query_batch, Neo4jTransactor.count))

    def wait_for_queues(self):
        Neo4jTransactor.queue.join()

    def run(self):
        logger.info("%s: Starting Neo4jTransactor Thread Runner: " % self._get_name())
        while True:

            (query_batch, query_counter) = Neo4jTransactor.queue.get()
            logger.info("%s: Processing query batch: %s BatchSize: %s" % (self._get_name(), query_counter, len(query_batch)))
            batch_start = time.time()

            for_query_counter = 0

            for neo4j_query, filename in query_batch:
                logger.info("%s: Processing query for file: %s QueryNum: %s QueueSize: %s" % (self._get_name(), filename, query_counter, Neo4jTransactor.queue.qsize()))
                start = time.time()
                try:
                    
                    if Neo4jTransactor.using_pickle == True:
                        # Save VIA pickle rather then NEO
                        file_name = "tmp/temp/transaction_%s_%s" % (query_counter, for_query_counter)
                        file = open(file_name,'wb')
                        logger.info("Writting to file: tmp/temp/transaction_%s_%s" % (query_counter, for_query_counter))
                        pickle.dump(neo4j_query, file)
                        file.close()
                    else:
                        session = Neo4jHelper.graph.session()
                        session.run(neo4j_query)
                        session.close()
                    
                    end = time.time()
                    elapsed_time = end - start
                    logger.info("%s: Processed query for file: %s QueryNum: %s QueueSize: %s Time: %s" % (self._get_name(), filename, query_counter, Neo4jTransactor.queue.qsize(), time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
                except Exception as e:
                    logger.error(e)
                    logger.error("%s: Query Failed: %s" % (self._get_name(), neo4j_query))
                    #logger.warn("%s: Query Conflict, putting data back in rework Queue. Size: %s Batch#: %s" % (self.threadid, Neo4jTransactor.rework.qsize(), batch_count))
                    #Neo4jTransactor.queue.put((generator, filename, query, batch_count))
                
                for_query_counter = for_query_counter + 1

            batch_end = time.time()
            batch_elapsed_time = batch_end - batch_start
            logger.info("%s: Query Batch finished: %s BatchSize: %s Time: %s" % (self._get_name(), query_counter, len(query_batch), time.strftime("%H:%M:%S", time.gmtime(batch_elapsed_time))))
            Neo4jTransactor.queue.task_done()