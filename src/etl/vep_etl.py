import logging
import multiprocessing
import uuid

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor
from data_manager import DataFileManager
from common import ContextInfo

logger = logging.getLogger(__name__)


class VEPETL(ETL):

    vep_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (a:Variant {primaryKey: row.hgvs_nomenclature})
                SET a.consequence = row.consequence

                """


    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):

        logger.info("Loading VEP Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        logger.info("Finished Loading Variation Data: %s" % sub_type.get_data_provider())

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [VEPETL.vep_query_template, commit_size, "vep_data_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)
        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, vep_data, data_provider, batch_size):

        if alleleRecord.get('alleleId') != 'MGI:6113870':
            variant_dataset = {
                "hgvs_nomenclature": hgvs_nomenclature,
                "genomicReferenceSequence": genomicReferenceSequence,
                "genomicVariantSequence": genomicVariantSequence,
                "paddingLeft": paddingLeft,
                "paddingRight": paddingRight,
                "alleleId": alleleRecord.get('alleleId'),
                "dataProviders": dataProviders,
                "dateProduced": dateProduced,
                "loadKey": loadKey,
                "release": release,
                "modGlobalCrossRefId": modGlobalCrossRefId,
                "dataProvider": data_provider
            }