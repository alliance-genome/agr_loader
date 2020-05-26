#!/usr/bin/env python
"""This is the main entry-point for running the ETL pipeline"""

import logging
import os
import multiprocessing
import time
import argparse
import coloredlogs

from etl import ETL, MIETL, DOETL, BGIETL, ConstructETL, ExpressionAtlasETL, GenericOntologyETL, \
                ECOMAPETL, AlleleETL, VariationETL, SequenceTargetingReagentETL, \
                AffectedGenomicModelETL, TranscriptETL, GOETL, ExpressionETL, ExpressionRibbonETL, \
                ExpressionRibbonOtherETL, DiseaseETL, PhenoTypeETL, OrthologyETL, ClosureETL, \
                GOAnnotETL, GeoXrefETL, GeneDiseaseOrthoETL, MolecularInteractionETL, \
                GeneDescriptionsETL, VEPETL, VEPTranscriptETL, Neo4jHelper, NodeCountETL

from transactors import Neo4jTransactor, FileTransactor
from data_manager import DataFileManager
from common import ContextInfo  # Must be the last timeport othersize program fails


def main():
    """ Entry point to ETL program"""

    parser = argparse.ArgumentParser(description=\
        'Load data into the Neo4j database for the Alliance of Genome Resources.')
    parser.add_argument('-c',
                        '--config',\
        help='Specify the filename of the YAML config. It must reside in the src/config/ directory',
                        default='default.yml')
    parser.add_argument('-v',
                        '--verbose',
                        help='Enable DEBUG mode for logging.',
                        action='store_true')
    args = parser.parse_args()

    # set context info
    context_info = ContextInfo()
    context_info.config_file_location = os.path.abspath('src/config/' + args.config)
    if args.verbose:
        context_info.env["DEBUG"] = True

    debug_level = logging.DEBUG if context_info.env["DEBUG"] else logging.INFO

    coloredlogs.install(level=debug_level,
                        fmt='%(asctime)s %(levelname)s: %(name)s:%(lineno)d: %(message)s',
                        field_styles={
                            'asctime': {'color': 'green'},
                            'hostname': {'color': 'magenta'},
                            'levelname': {'color': 'white', 'bold': True},
                            'name': {'color': 'blue'},
                            'programname': {'color': 'cyan'}
                        })

    logger = logging.getLogger(__name__)
    logging.getLogger("ontobio").setLevel(logging.ERROR)

    AggregateLoader(args, logger, context_info).run_loader()


class AggregateLoader():
    """This runs all the individiual ETL pipelines"""

    # This is the list of ETLs used for loading data.
    # The key (left) is derived from a value in the config YAML file.
    # The value (right) is hard-coded by a developer as the name of an ETL class.
    etl_dispatch = {
        'MI': MIETL,  # Special case. Grouped under "Ontology" but has a unique ETL.
        'DOID': DOETL,  # Special case. Grouped under "Ontology" but has a unique ETL.
        'BGI': BGIETL,
        'CONSTRUCT': ConstructETL,
        'GENEEEXPRESSIONATLASSITEMAP': ExpressionAtlasETL,
        'ONTOLOGY': GenericOntologyETL,
        'ECOMAP': ECOMAPETL,
        'ALLELE': AlleleETL,
        'VARIATION': VariationETL,
        'SQTR': SequenceTargetingReagentETL,
        'AGM': AffectedGenomicModelETL,
        'PHENOTYPE': PhenoTypeETL,
        'GFF': TranscriptETL,
        'GO': GOETL,
        'EXPRESSION': ExpressionETL,
        'ExpressionRibbon': ExpressionRibbonETL,
        'ExpressionRibbonOther': ExpressionRibbonOtherETL,
        'DAF': DiseaseETL,
        'ORTHO': OrthologyETL,
        'Closure': ClosureETL,
        'GAF': GOAnnotETL,
        'GEOXREF': GeoXrefETL,
        'GeneDiseaseOrtho': GeneDiseaseOrthoETL,
        'INTERACTION-MOL': MolecularInteractionETL,
        'GeneDescriptions': GeneDescriptionsETL,
        'VEP': VEPETL,
        'VEPTRANSCRIPT': VEPTranscriptETL,
        'DB-SUMMARY': NodeCountETL
    }

    # This is the order in which data types are loaded.
    # After each list, the loader will "pause" and wait for that item to finish.
    # i.e. After Ontology, there will be a pause.
    # After GO, DO, MI, there will be a pause, etc.
    etl_groups = [
        ['DOID', 'MI'],
        ['GO'],
        ['ONTOLOGY'],
        ['ECOMAP'],
        ['BGI'],
        ['CONSTRUCT'],
        ['ALLELE'],
        ['VARIATION'],
        ['SQTR'],
        ['AGM'],
        ['PHENOTYPE'],  # Locks Genes
        ['DAF'],  # Locks Genes
        ['ORTHO'],  # Locks Genes
        ['GeneDiseaseOrtho'],
        ['GFF'],
        ['EXPRESSION'],
        ['ExpressionRibbon'],
        ['ExpressionRibbonOther'],
        ['GENEEEXPRESSIONATLASSITEMAP'],
        ['GAF'],  # Locks Genes
        ['GEOXREF'],  # Locks Genes
        ['INTERACTION-MOL'],
        ['Closure'],
        ['GeneDescriptions'],
        ['VEP'],
        ['VEPTRANSCRIPT'],
        ['DB-SUMMARY']
    ]

    def __init__(self, args, logger, context_info):
        self.args = args
        self.logger = logger
        self.context_info = context_info
        self.start_time = time.time()

    @classmethod
    def run_etl_groups(cls, logger, data_manager, neo_transactor):
        """This function runs each of the ETL in parellel"""
        etl_time_tracker_list = []
        for etl_group in cls.etl_groups:
            etl_group_start_time = time.time()
            logger.info("Starting ETL group: %s" % etl_group)
            thread_pool = []
            for etl_name in etl_group:
                logger.info("ETL Name: %s" % etl_name)
                config = data_manager.get_config(etl_name)
                if config is not None:
                    etl = cls.etl_dispatch[etl_name](config)
                    process = multiprocessing.Process(target=etl.run_etl)
                    process.start()
                    thread_pool.append(process)
                else:
                    logger.info("No Config found for: %s" % etl_name)
            ETL.wait_for_threads(thread_pool)

            logger.info("Waiting for Queues to sync up")
            neo_transactor.check_for_thread_errors()
            neo_transactor.wait_for_queues()
            etl_elapsed_time = time.time() - etl_group_start_time
            etl_time_message = ("Finished ETL group: %s, Elapsed time: %s"
                                % (etl_group,
                                   time.strftime("%H:%M:%S", time.gmtime(etl_elapsed_time))))
            logger.info(etl_time_message)
            etl_time_tracker_list.append(etl_time_message)

        return etl_time_tracker_list


    def run_loader(self):
        """Main function for running loader"""

        if self.args.verbose:
            self.logger.warn('DEBUG mode enabled!')
            time.sleep(3)

        data_manager = DataFileManager(self.context_info.config_file_location)
        file_transactor = FileTransactor()

        file_transactor.start_threads(data_manager.get_file_transactor_thread_settings())

        data_manager.download_and_validate()
        self.logger.info("finished downloading now doing thread")

        file_transactor.check_for_thread_errors()
        self.logger.info("finished threads waiting for queues")

        file_transactor.wait_for_queues()
        self.logger.info("finished queues waiting for shutdown")
        file_transactor.shutdown()

        neo_transactor = Neo4jTransactor()
        neo_transactor.start_threads(data_manager.get_neo_transactor_thread_settings())

        self.logger.info("finished starting neo threads ")

        if not self.context_info.env["USING_PICKLE"]:
            self.logger.info("Creating indices.")
            Neo4jHelper.create_indices()

        etl_time_tracker_list = self.run_etl_groups(self.logger, data_manager, neo_transactor)

        neo_transactor.shutdown()

        elapsed_time = time.time() - self.start_time

        for time_item in etl_time_tracker_list:
            self.logger.info(time_item)

        self.logger.info('Loader finished. Elapsed time: %s'
                         % time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


if __name__ == '__main__':
    main()
