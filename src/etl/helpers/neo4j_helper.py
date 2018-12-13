import os, logging

from neo4j.v1 import GraphDatabase

logger = logging.getLogger(__name__)


class Neo4jHelper(object):

    @staticmethod
    def run_single_parameter_query(query, parameter):
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
        
        logger.debug("Running run_single_parameter_query. Please wait...")
        logger.debug("Query: %s" % query)
        with graph.session() as session:
            with session.begin_transaction() as tx:
                returnSet = tx.run(query, parameter=parameter)
        return returnSet
    
    @staticmethod
    def run_single_query(query):
        
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
        
        with graph.session() as session:
            with session.begin_transaction() as tx:
                returnSet = tx.run(query)
        return returnSet

    #def execute_transaction_batch(self, query, data, batch_size):
    #    logger.info("Executing batch query. Please wait...")
    #    logger.debug("Query: " + query)
    #    for submission in self.split_into_chunks(data, batch_size):
    #        self.execute_transaction(query, submission)
    #    logger.info("Finished batch loading.")

    #def split_into_chunks(self, data, batch_size):
    #    return (data[pos:pos + batch_size] for pos in range(0, len(data), batch_size))  