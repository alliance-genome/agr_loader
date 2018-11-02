from etl import ETL
import logging
from neo4j_transactor import Neo4jTransactor

logger = logging.getLogger(__name__)

class StubETL(ETL):

    query_template = """
        USING PERIODIC COMMIT 10000
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MERGE (n:Node {primaryKey:row.id})
            SET n.name = row.name """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        #for mod in mods
        #    json_data self.data_type_config.get_data()
        #    generator = self.get_generators(data)
        #    Neo4jTransactor.execute_transaction(generator, "so_data.csv", self.query)

        data = self.data_type_config.get_data()
        generator = self.get_generators(data)
        Neo4jTransactor.execute_transaction(generator, "neo4j_data.csv", StubETL.query_template)

    def get_generators(self, data):
        pass