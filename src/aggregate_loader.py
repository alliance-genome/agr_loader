import logging, coloredlogs, os, sys
from etl import *
from transactors import CSVTransactor, Neo4jTransactor, FileTransactor
from transactions import Indicies
from data_manager import DataFileManager


debug_level = logging.INFO

coloredlogs.install(level=debug_level,
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

# logging.basicConfig(stream=sys.stdout, level=logging.INFO,
# format='%(asctime)s %(levelname)s: %(name)s:%(lineno)d: %(message)s')

logger = logging.getLogger(__name__)


class AggregateLoader(object):

    def run_loader(self):

        # TODO Allow the yaml file location to be overwritten by command line input (for Travis).
        data_manager = DataFileManager(os.path.abspath('src/config/develop.yml'))
        data_manager.process_config()

        FileTransactor().start_threads(7)
        data_manager.download_and_validate()
        FileTransactor().wait_for_queues()

        Neo4jTransactor().start_threads(2)
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
            'GenericOntology': GenericOntologyETL,
            'Allele': AlleleETL,
            'Expression': ExpressionETL,
            'Disease': DiseaseETL,
            'Phenotype': PhenoTypeETL,
            'Orthology': OrthologyETL,
            'Ontology': GenericOntologyETL,
            'GOAnnot': GOAnnotETL,
            'GeoXref': GeoXrefETL,
            'ExpressionRibbon': ExpressionRibbonETL,
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
            ['GO', 'DO', 'SO', 'MI', 'ZFA', 'UBERON', 'BPSO', 'MMO', 'ZFS'],
            ['BGI'],
            ['Allele'],
            ['Expression'],
            ['Disease'], # Locks Genes
            ['Phenotype'], # Locks Genes
            ['Orthology'], # Locks Genes
            ['GOAnnot'], # Locks Genes
            ['GeoXref'], # Locks Genes
        ]

        for data_types in list_of_types:
            logger.debug("Data Types: %s" % data_types)
            for data_type in data_types:
                logger.debug("Data Type: %s" % data_type)
                config = data_manager.get_config(data_type)
                logger.debug("Config: %s" % config)
                if config is not None:
                    etl = etl_dispatch[data_type](config)
                    etl.run_etl()
                else:
                    logger.info("No Config found for: %s" % data_type)
            logger.info("Waiting for Queues to sync up")
            CSVTransactor().wait_for_queues()
            Neo4jTransactor().wait_for_queues()
            logger.info("Queue sync finished")

        # ETLs below get their data from an existent neo4j instance, rather than a file via the data manager
        ExpressionRibbonETL().run_etl()


if __name__ == '__main__':
    AggregateLoader().run_loader()
