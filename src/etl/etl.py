import logging
from neo4j_transactor import Neo4jTransactor

logger = logging.getLogger(__name__)

class ETL(object):

    def run_etl(self):
        self._load_and_process_data()
