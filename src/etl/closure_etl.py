"""Closure ETL."""

import logging
import multiprocessing

from etl import ETL
from transactors import CSVTransactor
from transactors import Neo4jTransactor
from .helpers import Neo4jHelper


class ClosureETL(ETL):
    """Clojure ETL."""

    logger = logging.getLogger(__name__)

    insert_isa_partof_closure_query_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row
                MATCH (termChild:%sTerm {primaryKey:row.child_id})
                MATCH (termParent:%sTerm {primaryKey:row.parent_id})
                CREATE (termChild)-[closure:IS_A_PART_OF_CLOSURE]->(termParent)
            }
        IN TRANSACTIONS of %s ROWS"""

    retrieve_isa_partof_closure_query_template = """
        MATCH (childTerm:%sTerm)-[:PART_OF|IS_A*]->(parentTerm:%sTerm)
            RETURN DISTINCT childTerm.primaryKey, parentTerm.primaryKey """

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
        data_provider = sub_type.get_data_provider()
        self.logger.info(data_provider)
        if data_provider == 'DOID':
            data_provider = 'DO'

        self.logger.debug("Starting isa_partof_ Closure for: %s", data_provider)

        query_list = [
            [self.insert_isa_partof_closure_query_template, "isa_partof_closure_" + data_provider + ".csv",
             data_provider, data_provider, "100000"],
        ]

        generators = self.get_closure_terms(data_provider)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

        self.error_messages("Closure-{}: ".format(data_provider))
        self.logger.debug("Finished isa_partof Closure for: %s", data_provider)

    def get_closure_terms(self, data_provider):
        """Get Closure Terms."""
        query = self.retrieve_isa_partof_closure_query_template % (data_provider, data_provider)
        self.logger.debug("Query to Run: %s", query)

        closure_data = []
        with Neo4jHelper().run_single_query(query) as return_set:
            for record in return_set:
                row = dict(child_id=record["childTerm.primaryKey"],
                           parent_id=record["parentTerm.primaryKey"])
                closure_data.append(row)

        yield [closure_data]
