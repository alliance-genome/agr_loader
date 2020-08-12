import logging
import multiprocessing
import uuid

from etl import ETL
from etl.helpers import ETLHelper
from etl.helpers import TextProcessingHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)


class HTPMetaDatasetSampleETL(ETL):
    htp_query_template = """
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

        logger.info("Loading HTP metadata Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        logger.info(filepath)
        data = JSONFile().get_data(filepath)
        logger.info("Finished Loading HTP metadata Data: %s" % sub_type.get_data_provider())

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [HTPMetaDatasetETL.htp_query_template, commit_size,
             "htp_metadataset_" + sub_type.get_data_provider() + ".csv"],
        ]

        # Obtain the generator
        generators = self.get_generators(data, batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, htp_dataset_data, batch_size):

        data_providers = []
        release = ""
        alleles_no_constrcut_no_gene = []
        counter = 0

        date_produced = allele_data['metaData']['dateProduced']

        loadKey = date_produced + data_provider + "_ALLELE"

        for allele_record in allele_data['data']:
            counter = counter + 1
            global_id = allele_record['primaryId']

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(global_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            gene_id = ''

            short_species_abbreviation = ETLHelper.get_short_species_abbreviation(allele_record.get('taxonId'))
            symbol_text = TextProcessingHelper.cleanhtml(allele_record.get('symbol'))

            if allele_record.get('alleleObjectRelations') is not None:
                for relation in allele_record.get('alleleObjectRelations'):
                    association_type = relation.get('objectRelation').get('associationType')
                    if relation.get('objectRelation').get('gene') is not None:
                        gene_id = relation.get('objectRelation').get('gene')
                    if relation.get('objectRelation').get('construct') is not None:
                        construct_id = relation.get('objectRelation').get('construct')

                    if gene_id != '' and construct_id != '':
                        allele_construct_gene_dataset = {
                            "symbol": allele_record.get('symbol'),
                            "geneId": gene_id,
                            "primaryId": allele_record.get('primaryId'),
                            "globalId": global_id,
                            "localId": local_id,
                            "taxonId": allele_record.get('taxonId'),
                            "dataProviders": data_providers,
                            "dateProduced": date_produced,
                            "loadKey": loadKey,
                            "release": release,
                            "modGlobalCrossRefId": mod_global_cross_ref_id,
                            "uuid": str(uuid.uuid4()),
                            "dataProvider": data_provider,
                            "symbolWithSpecies": allele_record.get('symbol') + " (" + short_species_abbreviation + ")",
                            "symbolTextWithSpecies": symbol_text + " (" + short_species_abbreviation + ")",
                            "symbolText": symbol_text,
                            "alleleDescription": description,
                            "constructId": construct_id,
                            "associationType": association_type
                        }
                        alleles_construct_gene.append(allele_construct_gene_dataset)

            if counter == batch_size:
                yield [alleles_no_construct, alleles_construct_gene, alleles_no_gene, alleles_no_constrcut_no_gene,
                       allele_secondary_ids, allele_synonyms, cross_reference_list]
                alleles_no_construct = []
                alleles_construct_gene = []
                alleles_no_gene = []
                alleles_no_constrcut_no_gene = []

                allele_secondary_ids = []
                allele_synonyms = []
                cross_reference_list = []
                counter = 0

        if counter > 0:
            yield [alleles_no_construct, alleles_construct_gene, alleles_no_gene, alleles_no_constrcut_no_gene,
                   allele_secondary_ids, allele_synonyms, cross_reference_list]
