import logging, sys, uuid
logger = logging.getLogger(__name__)

import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)


class ExpressionAtlasETL(ETL):

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MERGE (n:Node {primaryKey:row.id})
            SET n.name = row.name """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config
        self.expressionAtlasGenePages

    def _load_and_process_data(self):
        thread_pool = []
        filepath = self.data_type_config.get_single_filepath()
        logger.info(filepath)
#        for sub_type in self.data_type_config.get_sub_type_objects():
#            logger.info(sub_type)
#            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
#            p.start()
#            thread_pool.append(p)
#
#        ETL.wait_for_threads(thread_pool)
  
    def _process_sub_type(self, sub_type):

        filepath = sub_type.get_single_filepath()

#        commit_size = self.data_type_config.get_neo4j_commit_size()
#        batch_size = self.data_type_config.get_generator_batch_size()
#        
#        generators = self.get_generators(filepath, batch_size)
#
#        query_list = [
#            [StubETL.query_template, commit_size, "stub_data.csv"],
#        ]
#            
#        query_and_file_list = self.process_query_params(query_list)
#        CSVTransactor.save_file_static(generators, query_and_file_list)
#        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, filepath, batch_size):
        pass
