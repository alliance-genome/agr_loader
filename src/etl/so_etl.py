from itertools import islice, chain, tee
import logging
import sys

from etl import ETL
from transactors import CSVTransactor


logger = logging.getLogger(__name__)

class SOETL(ETL):

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MERGE (s:SOTerm:Ontology {primaryKey:row.id})
            SET s.name = row.name """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        data = self.data_type_config.get_data()

        commit_size = self.data_type_config.get_neo4j_commit_size()
        #batch_size = self.data_type_config.get_generator_batch_size()

        generators = self.get_generators(data)

        so_file_query_list = [[SOETL.query_template, commit_size, "so_term_data.csv"]]
            
        CSVTransactor.execute_transaction(generators, so_file_query_list)
        
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

        yield [so_list]


    def get_current_next(self, the_list):
        current, next_item = tee(the_list, 2)
        next_item = chain(islice(next_item, 1, None), [None])
        return zip(current, next_item)