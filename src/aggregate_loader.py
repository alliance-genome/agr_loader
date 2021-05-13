#!/usr/bin/env python
"""This is the main entry-point for running the ETL pipeline."""

import argparse
import logging
import multiprocessing
import os
import time
import coloredlogs
import json

from etl import (BGIETL, DOETL, ECOMAPETL, ETL, GOETL, MIETL, VEPETL,
                 AffectedGenomicModelETL, AlleleETL, BiogridOrcsXrefETL,
                 ClosureETL, ConstructETL, DiseaseETL, ExpressionAtlasETL,
                 ExpressionETL, ExpressionRibbonETL, ExpressionRibbonOtherETL,
                 GeneDescriptionsETL, GeneDiseaseOrthoETL, GenericOntologyETL,
                 GeoXrefETL, GOAnnotETL, GeneticInteractionETL,
                 MolecularInteractionETL, Neo4jHelper, NodeCountETL,
                 OrthologyETL, PhenoTypeETL, SequenceTargetingReagentETL,
                 SpeciesETL, TranscriptETL, VariationETL, VEPTranscriptETL,
                 ProteinSequenceETL, HTPMetaDatasetSampleETL,
                 HTPMetaDatasetETL, GenePhenoCrossReferenceETL,
                 CategoryTagETL)

from transactors import FileTransactor, Neo4jTransactor

from data_manager import DataFileManager
from files import Download
from loader_common import ContextInfo  # Must be the last timeport othersize program fails


def main():
    """Entry point to ETL program."""
    parser = argparse.ArgumentParser(
        description='Load data into the Neo4j database for the Alliance of Genome Resources.'
    )
    parser.add_argument(
        '-c',
        '--config', help='Specify the filename of the YAML config. It must reside in the src/config/ directory',
        default='default.yml'
    )
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
    """This runs all the individiual ETL pipelines."""

    # This is the list of ETLs used for loading data.
    # The key (left) is derived from a value in the config YAML file.
    # The value (right) is hard-coded by a developer as the name of an ETL class.
    etl_dispatch = {
        'SPECIES': SpeciesETL,
        'HTP': CategoryTagETL,
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
        'HTPDATASET': HTPMetaDatasetETL,
        'HTPDATASAMPLE': HTPMetaDatasetSampleETL,
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
        'BIOGRID-ORCS': BiogridOrcsXrefETL,
        'GeneDiseaseOrtho': GeneDiseaseOrthoETL,
        'INTERACTION-GEN': GeneticInteractionETL,
        'INTERACTION-MOL': MolecularInteractionETL,
        'GeneDescriptions': GeneDescriptionsETL,
        'VEPGENE': VEPETL,
        'VEPTRANSCRIPT': VEPTranscriptETL,
        'DB-SUMMARY': NodeCountETL,
        'ProteinSequence': ProteinSequenceETL,
        'GENEPHENOCROSSREFERENCE': GenePhenoCrossReferenceETL
    }

    # This is the order in which data types are loaded.
    # After each list, the loader will "pause" and wait for that item to finish.
    # i.e. After Ontology, there will be a pause.
    # After GO, DO, MI, there will be a pause, etc.
    etl_groups = [
        ['SPECIES'],
        ['HTP'],
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
        ['HTPDATASET'],
        ['HTPDATASAMPLE'],
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
        ['BIOGRID-ORCS'],  # Locks Genes
        ['INTERACTION-GEN'],
        ['INTERACTION-MOL'],
        ['Closure'],
        ['GeneDescriptions'],
        ['VEPGENE'],
        ['VEPTRANSCRIPT'],
        ['ProteinSequence'],
        ['GENEPHENOCROSSREFERENCE'],
        ['DB-SUMMARY']
    ]

    def __init__(self, args, logger, context_info):
        """Initialise object."""
        self.args = args
        self.logger = logger
        self.context_info = context_info
        self.start_time = time.time()
        context_info = ContextInfo()
        self.schema_branch = context_info.env["TEST_SCHEMA_BRANCH"]
        if self.schema_branch != 'master':
            self.logger.warning("*******WARNING: Using branch %s for schema.", self.schema_branch)

        # Lets delete the old files and download new ones. They are small.
        for name in ['tmp/species.yaml', 'tmp/resourceDescriptors.yaml']:
            if os.path.exists(name):
                self.logger.warning("*********WARNING: removing old %s file.", name)
                os.remove(name)
        self.logger.debug("Getting files initially")
        url = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/SCHEMA_BRANCH/resourceDescriptors.yaml'
        url = url.replace('SCHEMA_BRANCH', self.schema_branch)
        Download('tmp', url, 'resourceDescriptors.yaml').download_file()
        url = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/SCHEMA_BRANCH/ingest/species/species.yaml'
        url = url.replace('SCHEMA_BRANCH', self.schema_branch)
        Download('tmp', url, 'species.yaml').download_file()
        self.logger.debug("Finished getting files initially")

    @classmethod
    def run_etl_groups(cls, logger, data_manager, neo_transactor):
        """Run each of the ETLs in parallel."""
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
        """Run the loader."""
        if self.args.verbose:
            self.logger.warn('DEBUG mode enabled!')
            time.sleep(3)


        data_manager = DataFileManager(self.context_info.config_file_location)

        metadata = data_manager.get_release_info()
        fields = []
        for k in metadata:
            if 'Date' in k:
                fields.append(k + ': datetime("' + metadata[k] + '")')
            else:
                fields.append(k + ": " + json.dumps(metadata[k]))
        load_rel = "CREATE (o:AllianceSoftwareVersion {" + ",".join(fields) +"})"
        Neo4jHelper().run_single_query(load_rel)

        file_transactor = FileTransactor()
        file_transactor.start_threads(data_manager.get_file_transactor_thread_settings())

        data_manager.download_and_validate()
        self.logger.debug("finished downloading, now doing thread")

        file_transactor.check_for_thread_errors()
        self.logger.debug("finished threads, waiting for queues")

        file_transactor.wait_for_queues()
        self.logger.debug("finished queues, waiting for shutdown")
        file_transactor.shutdown()

        neo_transactor = Neo4jTransactor()
        neo_transactor.start_threads(data_manager.get_neo_transactor_thread_settings())

        self.logger.debug("finished starting neo threads ")

        if not self.context_info.env["USING_PICKLE"]:
            self.logger.info("Creating indices.")
            Neo4jHelper.create_indices()

        etl_time_tracker_list = self.run_etl_groups(self.logger, data_manager, neo_transactor)

        neo_transactor.shutdown()

        elapsed_time = time.time() - self.start_time

        for time_item in etl_time_tracker_list:
            self.logger.info(time_item)

        self.logger.info('Loader finished. Elapsed time: %s' % time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


if __name__ == '__main__':
    main()
