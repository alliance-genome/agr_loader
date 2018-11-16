import logging
logger = logging.getLogger(__name__)

from transactors import CSVTransactor

from etl import ETL

class StubETL(ETL):

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MERGE (n:Node {primaryKey:row.id})
            SET n.name = row.name """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()
        
        generators = self.get_generators(batch_size)

        query_list = [
            [StubETL.query_template, commit_size, "stub_data.csv"]
        ]
            
        CSVTransactor.execute_transaction(generators, query_list)

    def get_generators(self, batch_size):
        pass