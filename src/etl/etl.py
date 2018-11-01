import logging
from neo4j_transactor import Neo4jTransactor

logger = logging.getLogger(__name__)

class ETL(object):

    def run_etl(self):
        if self._running_etl():
            self._load_and_process_data()
            Neo4jTransactor.wait_for_queues()
