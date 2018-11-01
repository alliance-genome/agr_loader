from etl import ETL
import logging
from neo4j_transactor import Neo4jTransactor
from itertools import islice, chain, tee
import sys

logger = logging.getLogger(__name__)

class SOETL(ETL):

    query_template = """
        USING PERIODIC COMMIT 10000
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MERGE (s:SOTerm:Ontology {primaryKey:row.id})
            SET s.name = row.name """

    def __init__(self, data_manager):
        self.data_type_config = data_manager.get_config("SO")

    def _running_etl(self):
        return self.data_type_config != None and self.data_type_config.running_etl()

    def _load_and_process_data(self):
        #for mod in mods
        #    json_data self.data_type_config.get_data()
        #    generator = self.get_generators(data)
        #    Neo4jTransactor.execute_transaction(generator, "so_data.csv", self.query)

        data = self.data_type_config.get_data()
        generator = self.get_generators(data)
        Neo4jTransactor.execute_transaction(generator, "so_data.csv", SOETL.query_template)

    def get_generators(self, data):
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
                so_dataset = {
                'id' : value,
                'name' : next_value
                }
                so_list.append(so_dataset)
        yield so_list

    def get_current_next(self, the_list):
        current, next_item = tee(the_list, 2)
        next_item = chain(islice(next_item, 1, None), [None])
        return zip(current, next_item)