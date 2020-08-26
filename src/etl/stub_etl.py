"""Stub ETL."""

import logging
import multiprocessing

from etl import ETL
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class StubETL(ETL):
    """Stub ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MERGE (n:Node {primaryKey:row.id})
            SET n.name = row.name """

    # Querys which do not take params and can be used as is

    query = """
        MERGE (n:Node {primaryKey:row.id})
            SET n.name = row.name """

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):

        filepath = sub_type.get_single_filepath()

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        generators = self.get_generators(filepath, batch_size)

        query_template_list = [
            [self.query_template, commit_size, "stub_data.csv"],
        ]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("Stub-{}: ".format(sub_type.get_data_provider()))

    def get_generators(self, filepath, batch_size):
        """Get Generators."""
        generators = 1
        return generators
