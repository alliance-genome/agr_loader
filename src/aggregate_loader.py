import logging, coloredlogs, os, sys
from etl import *
from transactors import CSVTransactor, Neo4jTransactor, FileTransactor
from transactions import Indicies
from data_manager import DataFileManager

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

# logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s: %(name)s:%(lineno)d: %(message)s')
logger = logging.getLogger(__name__)

class AggregateLoader(object):

    def run_loader(self):

        # TODO Allow the yaml file location to be overwritten by command line input (for Travis).
        data_manager = DataFileManager(os.path.abspath('src/config/develop.yml'))
        data_manager.process_config()

        FileTransactor().start_threads(7)
        data_manager.download_and_validate()
        FileTransactor().wait_for_queues()

        Neo4jTransactor().start_threads(4)
        CSVTransactor().start_threads(7)
        
        if "USING_PICKLE" in os.environ and os.environ['USING_PICKLE'] == "True":
            pass
        else:
            logger.info("Creating indices.")
            Indicies().create_indices()

        etl_dispatch = {
            'GO': GOETL,
            'DO': DOETL,
            'SO': SOETL,
            'MI': MIETL,
            'BGI': BGIETL,
            'Allele': AlleleETL,
            'Expression': ExpressionETL,
            'Disease': DiseaseETL,
            'Phenotype': PhenoTypeETL,
            'Orthology': OrthologyETL,
            'Ontology': GenericOntologyETL,
            'GOAnnot': GOAnnotETL,
            #'ResourceDescriptor': ResourceDescriptorETL,
            #'MolecularInteraction': MolecularInteractionETL,
            #'GeneDiseaseOrthology': GeneDiseaseOrthologyETL,
        }

        # This is the order in which data types are loaded.
        # After each list, the loader will "pause" and wait for that item to finish.
        # i.e. After Ontology, there will be a pause.
        # After GO, DO, SO, MI, there will be a pause, etc.
        list_of_types = [
            ['Ontology'],
            ['GO', 'DO', 'SO', 'MI'],
            ['BGI'],
            #['Allele'],
            #['Expression'],
            #['Disease', 'Phenotype', 'Orthology'],
            #['GOAnnot'],
            #['GeoXref'],
        ]

        for data_types in list_of_types:
            for data_type in data_types:
                config = data_manager.get_config(data_type)
                if config is not None:
                    etl = etl_dispatch[data_type](config)
                    etl.run_etl()
            logger.info("Waiting for Queues to sync up")
            CSVTransactor().wait_for_queues()
            Neo4jTransactor().wait_for_queues()
            logger.info("Queue sync finished")

if __name__ == '__main__':
    AggregateLoader().run_loader()
