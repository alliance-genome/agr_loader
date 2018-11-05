import logging, coloredlogs
from loaders import *
from etl import *
from transactions import *
from neo4j_transactor import Neo4jTransactor
from data_file_manager import DataFileManager

coloredlogs.install(level=logging.INFO,
    fmt='%(asctime)s %(levelname)s: %(name)s:%(lineno)d: %(message)s',
    field_styles={
        'asctime': {'color': 'green'}, 
        'hostname': {'color': 'magenta'}, 
        'levelname': {'color': 'white', 'bold': True}, 
        'name': {'color': 'blue'}, 
        'programname': {'color': 'cyan'}
    })

# This has to be done because the OntoBio module does not use DEBUG it uses INFO which spews output.
# So we have to set the default to WARN in order to "turn off" OntoBio and then "turn on" by setting 
# to DEBUG the modules we want to see output for.
logger = logging.getLogger(__name__)

class AggregateLoader(object):

    def run_loader(self):

        # TODO Allow the yaml file location to be overwritten by command line input (for Travis).
        data_manager = DataFileManager('config/default.yml')
        # data_manager.process_config()
        # data_manager.download_and_validate()

        thread_pool = []
        for n in range(0, 4):
            runner = Neo4jTransactor()
            runner.threadid = n
            runner.daemon = True
            runner.start()
            thread_pool.append(runner)
        
        logger.info("Creating indices.")
        Indicies().create_indices()

        list_of_etls = {
            #'GO': GOETL,
            #'DO': DOETL,
            #'SO': SOETL,
            #'MI': MIETL,
            'BGI': BGIETL
            #'Allele': AlleleETL,
        }

        list_of_types = [
            ['BGI']
        ]

        for data_types in list_of_types:
            for data_type in data_types:
                config = data_manager.get_config(data_type)
                if config is not None:
                    etl = list_of_etls[data_type](config)
                    etl.run_etl()
            Neo4jTransactor().wait_for_queues()

if __name__ == '__main__':
    AggregateLoader().run_loader()
