"""Sequence Targetting Reagent ETL."""

import logging
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class SequenceTargetingReagentETL(ETL):
    """Sequence Targeting Reagent ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    sequence_targeting_reagent_query_template = """

    USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (s:Species {primaryKey: row.taxonId})

            //Create the Allele node and set properties. primaryKey is required.
            MERGE (o:SequenceTargetingReagent {primaryKey:row.primaryId})
                ON CREATE SET o.name = row.name,
                 o.dateProduced = row.dateProduced,
                 o.release = row.release,
                 o.localId = row.localId,
                 o.globalId = row.globalId,
                 o.uuid = row.uuid,
                 o.modCrossRefCompleteUrl = row.modGlobalCrossRefUrl,
                 o.dataProviders = row.dataProviders,
                 o.dataProvider = row.dataProvider

            MERGE (o)-[:FROM_SPECIES]-(s)
    """

    sequence_targeting_reagent_secondary_ids_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (f:SequenceTargetingReagent {primaryKey:row.primaryId})

            MERGE (second:SecondaryId:Identifier {primaryKey:row.secondaryId})
                SET second.name = row.secondary_id
            MERGE (f)-[aka1:ALSO_KNOWN_AS]->(second)
    """

    sequence_targeting_reagent_synonyms_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (a:SequenceTargetingReagent {primaryKey:row.primaryId})

            MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                SET syn.name = row.synonym
            MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn)
    """

    sequence_targeting_reagent_target_genes_query_template = """
    USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (a:SequenceTargetingReagent {primaryKey:row.primaryId})
            MATCH (g:Gene {primaryKey:row.geneId})

            MERGE (a)-[:TARGETS]-(g)
    """

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):

        self.logger.info("Loading Sequence Targeting Reagent Data: %s",
                         sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        self.logger.info(filepath)
        data = JSONFile().get_data(filepath)
        self.logger.info("Finished Loading Sequence Targeting Reagent Data: %s",
                         sub_type.get_data_provider())

        if data is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
            return
        ETLHelper.load_release_info(data, sub_type, self.logger)

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [self.sequence_targeting_reagent_query_template,
             commit_size, "str_data_" + sub_type.get_data_provider() + ".csv"],
            [self.sequence_targeting_reagent_secondary_ids_query_template, commit_size,
             "str_secondary_ids_" + sub_type.get_data_provider() + ".csv"],
            [self.sequence_targeting_reagent_synonyms_query_template, commit_size,
             "str_synonyms_" + sub_type.get_data_provider() + ".csv"],
            [self.sequence_targeting_reagent_target_genes_query_template, commit_size,
             "str_target_genes_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("STR-{}: ".format(sub_type.get_data_provider()))

    def secondary_process(self, secondarys, data_record):
        """Get secondary ids.

        secondarys: list of dataset items.
        data_record: record to process.
        """
        if data_record.get('secondaryIds') is None:
            return
        for sid in data_record.get('secondaryIds'):
            secondary_id_dataset = {
                "primaryId": data_record.get('primaryId'),
                "secondaryId": sid
            }
            secondarys.append(secondary_id_dataset)

    def synonyms_process(self, synonyms, data_record):
        """Get synonyms."""
        if data_record.get('synonyms') is None:
            return
        for syn in data_record.get('synonyms'):
            syn_dataset = {
                "primaryId": data_record.get('primaryId'),
                "synonym": syn.strip()
            }
            synonyms.append(syn_dataset)

    def get_generators(self, sqtr_data, data_provider, batch_size):  # noqa
        """Get Generators"""

        sqtrs = []
        sqtr_synonyms = []
        sqtr_secondary_ids = []
        mod_global_cross_ref_url = ""
        tgs = []

        counter = 0
        date_produced = sqtr_data['metaData']['dateProduced']

        load_key = date_produced + data_provider + "_SqTR"
        self.data_providers_process(sqtr_data)

        for sqtr_record in sqtr_data['data']:
            counter = counter + 1
            global_id = sqtr_record['primaryId']
            local_id = global_id.split(":")[1]

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(global_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue
            self.secondary_process(sqtr_secondary_ids, sqtr_record)
            self.synonyms_process(sqtr_synonyms, sqtr_record)

            if sqtr_record.get('targetGeneIds') is not None:
                for target_gene_id in sqtr_record.get('targetGeneIds'):
                    tg_dataset = {
                        "primaryId": sqtr_record.get('primaryId'),
                        "geneId": target_gene_id
                    }
                    tgs.append(tg_dataset)

            if 'crossReferences' in sqtr_record:

                for cross_ref in sqtr_record['modCrossReference']:
                    cross_ref_id = cross_ref.get('id')
                    local_crossref_id = cross_ref_id.split(":")[1]
                    prefix = cross_ref.get('id').split(":")[0]
                    pages = cross_ref.get('pages')

                    # some pages collection have 0 elements
                    if pages is None or len(pages) == 0:
                        continue
                    if 'sequence_targeting_reagent' in pages:
                        page = 'sequence_targeting_reagent'
                        mod_global_cross_ref_url = self.etlh.rdh2.return_url_from_key_value(
                            prefix, local_crossref_id, page)

            sqtr_dataset = {
                "primaryId": sqtr_record.get('primaryId'),
                "name": sqtr_record.get('name'),
                "globalId": global_id,
                "localId": local_id,
                "soTerm": sqtr_record.get('soTermId'),
                "taxonId": sqtr_record.get('taxonId'),
                "dataProviders": self.data_providers,
                "dateProduced": date_produced,
                "loadKey": load_key,
                "modGlobalCrossRefUrl": mod_global_cross_ref_url,
                "dataProvider": data_provider
            }
            sqtrs.append(sqtr_dataset)

            if counter == batch_size:
                yield [sqtrs, sqtr_secondary_ids, sqtr_synonyms, tgs]
                sqtrs = []
                sqtr_secondary_ids = []
                sqtr_synonyms = []
                tgs = []
                counter = 0

        if counter > 0:
            yield [sqtrs, sqtr_secondary_ids, sqtr_synonyms, tgs]
