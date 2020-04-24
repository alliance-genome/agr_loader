from etl import ETL
from files import TXTFile
from .helpers import Neo4jHelper
import logging
import os

logger = logging.getLogger(__name__)

class NodeCountETL(ETL):

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        filepath = self.data_type_config.get_single_filepath()

        production_datastore_generators = self.get_generators(filepath)
        current_datastore_generators = self.retrieve_node_counts_from_current_datastore()

        exception_filename = "labels_with_fewer_nodes.txt"
        file = open(os.path.join('tmp', exception_filename), 'w', encoding='utf-8')

        for current_db_node in current_datastore_generators:
            if current_db_node in production_datastore_generators:
                if self.testObject.using_test_data() is False:
                    if int(current_datastore_generators[current_db_node]) < int(production_datastore_generators[current_db_node]):
                        logger.warn("Node name: " + current_db_node + " current: "
                                                    + str(current_datastore_generators[current_db_node])
                                                    + " prod: "
                                                    + str(production_datastore_generators[current_db_node]))
                        file.write(current_db_node)


    def get_generators(self, filepath):

        data = TXTFile(filepath).get_data()
        node_count_dataset = {}
        # TODO: AGR-2121
        for line in data:
            columns = line.split(":")
            if len(columns) > 1:
                node_name = columns[0].replace('"', "").strip()
                node_count = columns[1].strip().rstrip()
                if node_count == "{":
                    continue
                else:
                    node_count = node_count.replace(",","")
                    node_count_dataset[node_name] = node_count
        return node_count_dataset

    def retrieve_node_counts_from_current_datastore (self):

        logger.info("retrieve node counts")

        retrieve_node_count = """
                CALL db.labels() YIELD label CALL apoc.cypher.run('MATCH (:`'+label+'`)
                                RETURN count(*) as count',{})
                                YIELD value RETURN label as label, value.count as node_count order by label
        """

        returnSet = Neo4jHelper().run_single_query(retrieve_node_count)

        current_node_count = {}

        for record in returnSet:
            current_node_count[record['label']] = record['node_count']

        return current_node_count
