
import logging
from transactions import Transaction

logger = logging.getLogger(__name__)

class SOETL(ETL):

    def __init__(self, etl_config):
        self.config = etl_config.getSoConfig()
        self.query = """
            UNWIND $data as row %s
            MERGE (s:SOTerm:Ontology {primaryKey:row.id})
                SET s.name = row.name """

    def _load_data_file(self):
        return self.config.get_data()

    def _running_etl():
        return True

    def _process_data(self, data):
        generator = get_generators(data)
        Transaction.queue.add(generator, "so_data.csv", self.query)

    def get_generators(self, data):
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
        return so_list

    def get_current_next(self, the_list):
        current, next_item = tee(the_list, 2)
        next_item = chain(islice(next_item, 1, None), [None])
        return zip(current, next_item)