import logging
from transactions import Transaction

logger = logging.getLogger(__name__)

class ETL(object):

    def run_etl(self):
        if _running_etl():
            _process_data(_load_data_file())
            Transaction.queue.join()
