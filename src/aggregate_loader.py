import logging, coloredlogs, os, multiprocessing, time, argparse, time
from etl import *
from etl.helpers import Neo4jHelper
from transactors import Neo4jTransactor, FileTransactor
from data_manager import DataFileManager
from common import ContextInfo

parser = argparse.ArgumentParser(description='Load data into the Neo4j database for the Alliance of Genome Resources.')
parser.add_argument('-c', '--config', help='Specify the filename of the YAML config. It must reside in the src/config/ directory', default='default.yml')
parser.add_argument('-v', '--verbose', help='Enable DEBUG mode for logging.', action='store_true')
args = parser.parse_args()

if args.verbose:
    debug_level = logging.DEBUG
else:
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

        if args.verbose:
            logger.warn('DEBUG mode enabled!')
            time.sleep(3)

        start_time = time.time()

        context_info = ContextInfo()
        context_info.config_file_location = os.path.abspath('src/config/' + args.config)
        context_info.verbose = args.verbose
        data_manager = DataFileManager(context_info.config_file_location)

        ft = FileTransactor()

        ft.start_threads(data_manager.get_FT_thread_settings())
        data_manager.download_and_validate()
        ft.wait_for_queues()
        ft.shutdown()
        
        nt = Neo4jTransactor()

        nt.start_threads(data_manager.get_NT_thread_settings())
        
        if "USING_PICKLE" in os.environ and os.environ['USING_PICKLE'] == "True":
            pass
        else:
            logger.info("Creating indices.")
            Neo4jHelper.create_indices()

        # This is the list of ETLs used for loading data.
        # The key (left) is derived from a value in the config YAML file.
        # The value (right) is hard-coded by a developer as the name of an ETL class.
        etl_dispatch = {
            'SO': SOETL,  # Special case. Grouped under "Ontology" but has a unique ETL.
            'MI': MIETL,  # Special case. Grouped under "Ontology" but has a unique ETL.
            'DO': DOETL,  # Special case. Grouped under "Ontology" but has a unique ETL.
            'BGI': BGIETL,
            'Ontology': GenericOntologyETL,
            'Allele': AlleleETL,
            'GO': GOETL,
            'Expression': ExpressionETL,
            'ExpressionRibbon': ExpressionRibbonETL,
            'Disease': DiseaseETL,
            'Phenotype': PhenoTypeETL,
            'Orthology': OrthologyETL,
            'Closure': ClosureETL,
            'GOAnnot': GOAnnotETL,
            'GeoXref': GeoXrefETL,
            'GeneDiseaseOrtho': GeneDiseaseOrthoETL,
            'Interactions': MolecularInteractionETL            
        }

        # This is the order in which data types are loaded.
        # After each list, the loader will "pause" and wait for that item to finish.
        # i.e. After Ontology, there will be a pause.
        # After GO, DO, SO, MI, there will be a pause, etc.
        list_of_etl_groups = [
            ['DO', 'SO', 'MI'],
            ['GO'],
            ['Ontology'],
            ['BGI'],
            ['Allele'],
            ['Expression'],
            ['ExpressionRibbon'],
            ['Disease'],  # Locks Genes
            ['Phenotype'],  # Locks Genes
            ['Orthology'],  # Locks Genes
            ['GOAnnot'],  # Locks Genes
            ['GeoXref'],  # Locks Genes
            ['GeneDiseaseOrtho'],
            ['Interactions'],
            ['Closure']
        ]
        etl_time_tracker_list = []

        for etl_group in list_of_etl_groups:
            etl_group_start_time = time.time()
            logger.info("Starting ETL group: %s" % etl_group)
            thread_pool = []
            for etl_name in etl_group:
                logger.debug("ETL Name: %s" % etl_name)
                config = data_manager.get_config(etl_name)
                logger.debug("Config: %s" % config)
                if config is not None:
                    etl = etl_dispatch[etl_name](config)
                    p = multiprocessing.Process(target=etl.run_etl)
                    p.start()
                    thread_pool.append(p)
                else:
                    logger.info("No Config found for: %s" % etl_name)
            for thread in thread_pool:
                thread.join()
                
            logger.info("Waiting for Queues to sync up")
            Neo4jTransactor().wait_for_queues()
            etl_elapsed_time = time.time() - etl_group_start_time
            etl_time_message = ("Finished ETL group: %s, Elapsed time: %s" % (etl_group, time.strftime("%H:%M:%S", time.gmtime(etl_elapsed_time))))
            logger.info(etl_time_message)
            etl_time_tracker_list.append(etl_time_message)

        nt.shutdown()

        end_time = time.time()
        elapsed_time = end_time - start_time

        for time_item in etl_time_tracker_list:
            logger.info(time_item)
        logger.info('Loader finished. Elapsed time: %s' % time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


if __name__ == '__main__':
    AggregateLoader().run_loader()
