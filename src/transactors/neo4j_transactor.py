'''Neo4j Transacotr'''

import logging
import multiprocessing
import pickle
import time
from neo4j import GraphDatabase
from etl import ETL
from common import ContextInfo


class Neo4jTransactor():
    '''Neo4j Transactor'''

    logger = logging.getLogger(__name__)
    count = 0
    queue = None

    def __init__(self):
        self.thread_pool = []


    @staticmethod
    def _get_name():
        return "Neo4jTransactor %s" % multiprocessing.current_process().name


    def start_threads(self, thread_count):
        '''Start Threads'''

        manager = multiprocessing.Manager()
        queue = manager.Queue()
        Neo4jTransactor.queue = queue

        for i in range(0, thread_count):
            process = multiprocessing.Process(target=self.run, name=str(i))
            process.start()
            self.thread_pool.append(process)


    def shutdown(self):
        '''Shutdown'''

        self.logger.info("Shutting down Neo4jTransactor threads: %s", len(self.thread_pool))
        for thread in self.thread_pool:
            thread.terminate()
        self.logger.info("Finished Shutting down Neo4jTransactor threads")


    @staticmethod
    def execute_query_batch(query_batch):
        '''Execture Query Batch'''

        Neo4jTransactor.count = Neo4jTransactor.count + 1
        Neo4jTransactor.logger.debug("Adding Query Batch: %s BatchSize: %s QueueSize: %s ",
                                     Neo4jTransactor.count,
                                     len(query_batch),
                                     Neo4jTransactor.queue.qsize())
        Neo4jTransactor.queue.put((query_batch, Neo4jTransactor.count))

    def check_for_thread_errors(self):
        '''Check for Thread Errors'''

        ETL.wait_for_threads(self.thread_pool, Neo4jTransactor.queue)

    @staticmethod
    def wait_for_queues():
        '''Wait for Queues'''

        Neo4jTransactor.queue.join()

    def run(self):
        '''Run'''

        context_info = ContextInfo()

        if context_info.env["USING_PICKLE"] is False:
            uri = "bolt://" + context_info.env["NEO4J_HOST"] \
                    + ":" + str(context_info.env["NEO4J_PORT"])
            graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1)

        self.logger.info("%s: Starting Neo4jTransactor Thread Runner: ", self._get_name())
        while True:
            try:
                (query_batch, query_counter) = Neo4jTransactor.queue.get()
            except EOFError as error:
                self.logger.info("Queue Closed exiting: %s", error)
                return

            self.logger.debug("%s: Processing query batch: %s BatchSize: %s",
                              self._get_name(),
                              query_counter,
                              len(query_batch))
            batch_start = time.time()

            total_query_counter = 0

            while len(query_batch) > 0:

                (neo4j_query, filename) = query_batch.pop(0)

                self.logger.debug("%s: Processing query for file: %s QueryNum: %s QueueSize: %s",
                                  self._get_name(),
                                  filename,
                                  query_counter,
                                  Neo4jTransactor.queue.qsize())
                start = time.time()
                try:
                    if context_info.env["USING_PICKLE"] is True:
                        # Save VIA pickle rather then NEO
                        file_name = "tmp/temp/transaction_%s_%s" \
                                     % (query_counter, total_query_counter)
                        with open(file_name, 'wb') as file:
                            self.logger.debug("Writting to file: tmp/temp/transaction_%s_%s",
                                              query_counter,
                                              total_query_counter)
                            pickle.dump(neo4j_query, file)
                    else:
                        session = graph.session()
                        session.run(neo4j_query)
                        session.close()

                    end = time.time()
                    elapsed_time = end - start
                    self.logger.info("%s: Processed query for file: %s QueryNum: %s QueueSize: %s Time: %s",
                                     self._get_name(),
                                     filename,
                                     query_counter,
                                     Neo4jTransactor.queue.qsize(),
                                     time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
                except Exception as error:
                    self.logger.error(error)
                    #logger.error("%s: Query Failed: %s" % (self._get_name(), neo4j_query))
                    # TODO Extract and print NODE information from error message.
                    # Would be helpful for troubleshooting.
                    self.logger.warning("%s: Query Conflict, putting data back in Queue to run later. %s",
                                        self._get_name(),
                                        filename)
                    query_batch.insert(0, (neo4j_query, filename))
                    time.sleep(12)
                    Neo4jTransactor.queue.put((query_batch, query_counter))
                    break

                total_query_counter = total_query_counter + 1

            batch_end = time.time()
            batch_elapsed_time = batch_end - batch_start
            self.logger.debug("%s: Query Batch finished: %s BatchSize: %s Time: %s",
                              self._get_name(),
                              query_counter,
                              len(query_batch),
                              time.strftime("%H:%M:%S", time.gmtime(batch_elapsed_time)))
            Neo4jTransactor.queue.task_done()
