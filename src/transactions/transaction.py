import logging
import os
from queue import Queue
from threading import Thread
import time

from neo4j import GraphDatabase


logger = logging.getLogger(__name__)

class Transaction(Thread):

    count = 0
    queue = Queue(20)
    rework = Queue(0)

    if "NEO4J_NQC_HOST" in os.environ:
        host = os.environ['NEO4J_NQC_HOST']
    else:
        host = "localhost"
        
    if "NEO4J_NQC_PORT" in os.environ:
        port = int(os.environ['NEO4J_NQC_PORT'])
    else:
        port = 7687

    if "USING_PICKLE" in os.environ and os.environ['USING_PICKLE'] == "True":
        pass
    else:
        uri = "bolt://" + host + ":" + str(port)
        graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1)

    def __init__(self):
        Thread.__init__(self)
        self.threadid = 0

    def run_single_query(self, query):
        with self.graph.session() as session:
            with session.begin_transaction() as tx:
                returnSet = tx.run(query)
        return returnSet

    def execute_transaction(self, query, data):
        Transaction.count = Transaction.count + 1
        Transaction.queue.put((query, data, Transaction.count))
        logger.info("Adding Items Batch: %s QueueSize: %s DataSize: %s" % (Transaction.count, Transaction.queue.qsize(), len(data)))

    def run_single_parameter_query(self, query, parameter):
        logger.debug("Running run_single_parameter_query. Please wait...")
        logger.debug("Query: %s" % query)
        with Transaction.graph.session() as session:
            with session.begin_transaction() as tx:
                returnSet = tx.run(query, parameter=parameter)
        return returnSet

    def execute_transaction_batch(self, query, data, batch_size):
        logger.info("Executing batch query. Please wait...")
        logger.debug("Query: " + query)
        for submission in self.split_into_chunks(data, batch_size):
            self.execute_transaction(query, submission)
        logger.info("Finished batch loading.")

    def split_into_chunks(self, data, batch_size):
        return (data[pos:pos + batch_size] for pos in range(0, len(data), batch_size))

    def wait_for_queues(self):
        Transaction.rework.join()
        Transaction.queue.join()

    def run(self):
        logger.info("%s: Starting Transaction Thread Runner: " % self.threadid)
        while True:
            rework = False
            if Transaction.rework.qsize() > 0:
                logger.info("%s: Pulling rework: " % (self.threadid))
                (cypher_query, query_data, batch_count) = Transaction.rework.get()
                logger.info("%s: Proceeding with rework batch: %s" % (self.threadid, batch_count))
                rework = True
            else:
                (cypher_query, query_data, batch_count) = Transaction.queue.get()
            
            #logger.info("%s Removing item: %s from queue: %s" % (batch_count, len(query_data), Transaction.queue.qsize()))
            start = time.time()
            
            # Save VIA pickle rather then NEO
            #file_name = "tmp/transaction_%s" % batch_count
            #file = open(file_name,'wb')
            #logger.info("Writting to file: %s Records: %s" % (file_name, len(query_data)))
            #pickle.dump((cypher_query, query_data), file)
            #file.close() 
            
            try:
                #Transaction.graph.session().run(cypher_query, data=query_data)
                #logger.info("%s: Thread processing data from Queue: %s" % (self.threadid, Transaction.queue.qsize()))
                session = Transaction.graph.session()
                tx = session.begin_transaction()
                if query_data != None:
                    tx.run(cypher_query, data=query_data)
                else:
                    tx.run(cypher_query)

                tx.commit()
                session.close()
                
                end = time.time()
                #logger.info("%s: Time Diff: %s" % (self.threadid, end - last_tx))
                logger.info("%s: Processed %s entries. %s r/s QueueSize: %s BatchNum: %s" % (self.threadid, len(query_data), round((len(query_data) / (end - start)), 2), Transaction.queue.qsize(), batch_count))
            except:
                logger.warn("%s: Query Conflict, putting data back in rework Queue. Size: %s Batch#: %s" % (self.threadid, Transaction.rework.qsize(), batch_count))
                Transaction.rework.put((cypher_query, query_data, batch_count))

            if rework:
                Transaction.rework.task_done()
            else:
                Transaction.queue.task_done()
