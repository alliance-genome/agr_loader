"""Affected Genomic Model ETL."""

import logging
import multiprocessing

from etl import ETL
from etl.helpers import TextProcessingHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor


class AffectedGenomicModelETL(ETL):
    """ETL for adding Affected Genomic Model."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    agm_query_template = """

    USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (s:Species {primaryKey: row.taxonId})

            MERGE (o:AffectedGenomicModel {primaryKey:row.primaryId})
                ON CREATE SET o.name = row.name,
                o.nameText = row.nameText,
                 o.dateProduced = row.dateProduced,
                 o.release = row.release,
                 o.localId = row.localId,
                 o.globalId = row.globalId,
                 o.uuid = row.uuid,
                 o.modCrossRefCompleteUrl = row.modGlobalCrossRefUrl,
                 o.dataProviders = row.dataProviders,
                 o.dataProvider = row.dataProvider,
                 o.nameText = row.nameText,
                 o.nameTextWithSpecies = row.nameTextWithSpecies,
                 o.nameWithSpecies = row.nameWithSpecies,
                 o.subtype = row.subtype

            MERGE (o)-[:FROM_SPECIES]-(s)
    """

    agm_secondary_ids_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (f:AffectedGenomicModel {primaryKey:row.primaryId})

                MERGE (second:SecondaryId:Identifier {primaryKey:row.secondaryId})
                    SET second.name = row.secondary_id
                MERGE (f)-[aka1:ALSO_KNOWN_AS]->(second)

        """

    agm_sqtrs_query_template = """
        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (sqtr:SequenceTargetingReagent {primaryKey:row.sqtrId})
            MATCH (agm:AffectedGenomicModel {primaryKey:row.primaryId})

            MERGE (agm)-[:SEQUENCE_TARGETING_REAGENT]-(sqtr)
    """

    agm_synonyms_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (a:AffectedGenomicModel {primaryKey:row.primaryId})

                MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                    SET syn.name = row.synonym
                MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn)

        """

    agm_backgrounds_query_template = """
     USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (agm:AffectedGenomicModel {primaryKey:row.primaryId})
            MATCH (b:AffectedGenomicModel {primaryKey:row.backgroundId})

            MERGE (agm)-[:BACKGROUND]-(b)

    """

    agm_components_query_template = """
     USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (feature:Feature:Allele {primaryKey:row.componentId})
            MATCH (agm:AffectedGenomicModel {primaryKey:row.primaryId})

            MERGE (agm)-[agmf:MODEL_COMPONENT]-(feature)
                SET agmf.zygosity = row.zygosityId

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

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [self.agm_query_template, commit_size,
             "agm_data_" + sub_type.get_data_provider() + ".csv"],
            [self.agm_secondary_ids_query_template, commit_size,
             "agm_secondary_ids_" + sub_type.get_data_provider() + ".csv"],
            [self.agm_synonyms_query_template, commit_size,
             "agm_synonyms_" + sub_type.get_data_provider() + ".csv"],
            [self.agm_components_query_template, commit_size,
             "agm_components_" + sub_type.get_data_provider() + ".csv"],
            [self.agm_sqtrs_query_template, commit_size,
             "agm_sqtrs_" + sub_type.get_data_provider() + ".csv"],
            [self.agm_backgrounds_query_template, commit_size,
             "agm_backgrounds_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("AGM-{}: ".format(sub_type.get_data_provider()))

    def cross_ref_process(self, agm_record):
        """Get cross reference."""
        cross_ref = ""
        if 'crossReference' not in agm_record:
            return cross_ref
        cross_ref = agm_record.get('crossReference')
        cross_ref_id = cross_ref.get('id')
        local_crossref_id = cross_ref_id.split(":")[1]
        prefix = cross_ref.get('id').split(":")[0]
        pages = cross_ref.get('pages')

        # some pages collection have 0 elements
        if pages is not None and len(pages) > 0:
            for page in pages:
                if page in ['Fish', 'genotype', 'strain']:
                    cross_ref = self.etlh.rdh2.return_url_from_key_value(
                        prefix, local_crossref_id, alt_page=page)
        return cross_ref

    def agm_process(self, agms, agm_record, data_provider, data_providers, date_produced):
        """Process agms."""
        # TODO: make subtype required in submission file.
        subtype = agm_record.get('subtype')
        if subtype is None and data_provider == 'WB':
            subtype = 'strain'
        if subtype is None:
            subtype = 'affected_genomic_model'

        global_id = agm_record['primaryID']
        local_id = global_id.split(":")[1]
        short_species_abbreviation = self.etlh.get_short_species_abbreviation(agm_record.get('taxonId'))
        name_text = TextProcessingHelper.cleanhtml(agm_record.get('name'))
        mod_global_cross_ref_url = self.cross_ref_process(agm_record)
        load_key = date_produced + data_provider + "_agm"
        # TODO: name_text
        agm_dataset = {
                "primaryId": agm_record.get('primaryID'),
                "name": agm_record.get('name'),
                "globalId": global_id,
                "localId": local_id,
                "taxonId": agm_record.get('taxonId'),
                "dataProviders": data_providers,
                "dateProduced": date_produced,
                "loadKey": load_key,
                "subtype": subtype,
                "modGlobalCrossRefUrl": mod_global_cross_ref_url,
                "dataProvider": data_provider,
                "nameText": name_text,
                "nameWithSpecies": agm_record.get('name') + " (" + short_species_abbreviation + ")",
                "nameTextWithSpecies": name_text + " (" + short_species_abbreviation + ")",
        }
        agms.append(agm_dataset)

    def genmod_process(self, components, agm_record):
        """Process affected genomic Model components."""
        if agm_record.get('affectedGenomicModelComponents') is None:
            return
        for component in agm_record.get('affectedGenomicModelComponents'):
            component_dataset = {
                "primaryId": agm_record.get('primaryID'),
                "componentId": component.get('alleleID'),
                "zygosityId": component.get('zygosity')
                }
            components.append(component_dataset)

    def ppids_process(self, backgrounds, agm_record):
        """Parental Pop Ids process."""
        if agm_record.get('parentalPopulationIDs') is None:
            return
        for background in agm_record.get('parentalPopulationIDs'):
            background_dataset = {
                "primaryId": agm_record.get('primaryID'),
                "backgroundId": background
            }
            backgrounds.append(background_dataset)

    def sqtr_process(self, sqtrs, agm_record):
        """Get sqtrs."""
        if agm_record.get('sequenceTargetingReagentIDs') is None:
            return
        for sqtr in agm_record.get('sequenceTargetingReagentIDs'):
            sqtr_dataset = {
                "primaryId": agm_record.get('primaryID'),
                "sqtrId": sqtr
            }
            sqtrs.append(sqtr_dataset)

    def secondary_process(self, secondarys, data_record):
        """Get secondary ids.

        secondarys: list of dataset items.
        data_record: record to process.
        """
        if data_record.get('secondaryIds') is None:
            return
        for sid in data_record.get('secondaryIds'):
            secondary_id_dataset = {
                "primaryId": data_record.get('primaryID'),
                "secondaryId": sid
            }
            secondarys.append(secondary_id_dataset)

    def synonyms_process(self, synonyms, data_record):
        """Get synonyms."""
        if data_record.get('synonyms') is None:
            return
        for syn in data_record.get('synonyms'):
            syn_dataset = {
                "primaryId": data_record.get('primaryID'),
                "synonym": syn.strip()
            }
            synonyms.append(syn_dataset)

    def get_generators(self, agm_data, data_provider, batch_size):
        """Get Generators."""
        data_providers = []
        agms = []
        agm_synonyms = []
        agm_secondary_ids = []
        components = []
        backgrounds = []
        sqtrs = []

        counter = 0
        date_produced = agm_data['metaData']['dateProduced']

        data_provider_object = agm_data['metaData']['dataProvider']

        data_provider_cross_ref = data_provider_object.get('crossReference')
        data_provider = data_provider_cross_ref.get('id')
        data_provider_pages = data_provider_cross_ref.get('pages')
        data_provider_cross_ref_set = []

        self.data_providers_process(data_provider, data_providers,
                                    data_provider_pages, data_provider_cross_ref_set)

        for agm_record in agm_data['data']:
            counter = counter + 1
            global_id = agm_record['primaryID']

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(global_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            self.secondary_process(agm_secondary_ids, agm_record)
            self.synonyms_process(agm_synonyms, agm_record)
            self.agm_process(agms, agm_record, data_provider, data_providers, date_produced)
            self.genmod_process(components, agm_record)
            self.sqtr_process(sqtrs, agm_record)
            self.ppids_process(backgrounds, agm_record)

            if counter == batch_size:
                yield [agms, agm_secondary_ids, agm_synonyms, components, sqtrs, backgrounds]
                agms = []
                agm_secondary_ids = []
                agm_synonyms = []
                components = []
                backgrounds = []
                counter = 0

        if counter > 0:
            yield [agms, agm_secondary_ids, agm_synonyms, components, sqtrs, backgrounds]
