import logging
from neo4j_transactor import Neo4jTransactor

logger = logging.getLogger(__name__)

class ETL(object):

    def run_etl(self):
        if _running_etl():
            _process_data(_load_data_file())
            Neo4jTransactor.wait_for_queues()
