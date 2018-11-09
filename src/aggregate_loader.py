import logging, coloredlogs
from etl import *
from transactors import CSVTransactor, Neo4jTransactor
from transactions import Indicies
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

        Neo4jTransactor().start_threads(1)
        CSVTransactor().start_threads(4)
        
        logger.info("Creating indices.")
        Indicies().create_indices()

        list_of_etls = {
            'GO': GOETL,
            'DO': DOETL,
            'SO': SOETL,
            'MI': MIETL,
            'BGI': BGIETL,
            'Allele': AlleleETL,
            'Expression': ExpressionETL,
            'DiseaseAllele': DiseaseAlleleETL,
        }

        list_of_types = [
            ['GO', 'DO', 'SO', 'MI'],
            #['BGI'],
            #['Allele'],
            # ['Expression'],
            #['DiseaseAllele'],
            #['DiseaseGene'],
            #['Phenotype'],
            #['Orthology'],
            #['GOAnnot'],
            #['GeoXref'],
        ]

        for data_types in list_of_types:
            for data_type in data_types:
                config = data_manager.get_config(data_type)
                if config is not None:
                    etl = list_of_etls[data_type](config)
                    etl.run_etl()
            logger.info("Waiting for Queues to sync up")
            CSVTransactor().wait_for_queues()
            Neo4jTransactor().wait_for_queues()
            logger.info("Queue sync finished")

if __name__ == '__main__':
    AggregateLoader().run_loader()
