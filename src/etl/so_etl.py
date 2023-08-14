"""SO ETL."""

import sys
import logging

from itertools import islice, chain, tee
from etl import ETL
from etl.helpers import OBOHelper
from files import TXTFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class SOETL(ETL):
    """SO ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    main_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MERGE (s:SOTerm {primaryKey:row.id})
                    ON CREATE SET s :Ontology
                    SET s.name = row.name

                    MERGE (s)-[ggcg:IS_A_PART_OF_CLOSURE]->(s)
            }
        IN TRANSACTIONS of %s ROWS"""

    def __init__(self, config):
        """Initialise iobject."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        filepath = self.data_type_config.get_single_filepath()

        commit_size = self.data_type_config.get_neo4j_commit_size()

        generators = self.get_generators(filepath)

        query_template_list = [[self.main_query_template, "so_term_data.csv", commit_size]]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("SO: ")

    def get_generators(self, filepath):
        """Get Generators."""

        OBOHelper.add_metadata_to_neo(filepath)
        data = TXTFile(filepath).get_data()
        so_list = []
        for current_line, next_line in self.get_current_next(data):
            so_dataset = {}
            current_line = current_line.strip()
            key = (current_line.split(":")[0]).strip()
            if key == "id":
                value = ("".join(":".join(current_line.split(":")[1:]))).strip()
                if not value.startswith('SO'):
                    continue
                next_key = (next_line.split(":")[0]).strip()
                if next_key == "name":
                    next_value = ("".join(":".join(next_line.split(":")[1:]))).strip()
                else:
                    sys.exit("FATAL ERROR: Expected SO name not found for %s" % (key))
                so_dataset = {'id': value,
                              'name': next_value}
                so_list.append(so_dataset)

        yield [so_list]

    @classmethod
    def get_current_next(cls, the_list):
        """Get Current Next."""
        current, next_item = tee(the_list, 2)
        next_item = chain(islice(next_item, 1, None), [None])

        return zip(current, next_item)
