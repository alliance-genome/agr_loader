"""'Affected Genomic Model ETL"""

import logging
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from etl.helpers import TextProcessingHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor


class AffectedGenomicModelETL(ETL):
    """ETL for adding Affected Genomic Model"""

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


    def get_generators(self, agm_data, data_provider, batch_size):
        """Get Generators"""

        data_providers = []
        agms = []
        agm_synonyms = []
        agm_secondary_ids = []
        mod_global_cross_ref_url = ""
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

        load_key = date_produced + data_provider + "_agm"

        if data_provider_pages is not None:
            for data_provider_page in data_provider_pages:
                cross_ref_complete_url = ETLHelper.get_page_complete_url(data_provider,
                                                                         self.xref_url_map,
                                                                         data_provider,
                                                                         data_provider_page)

                data_provider_cross_ref_set.append(ETLHelper.get_xref_dict(\
                        data_provider,
                        data_provider,
                        data_provider_page,
                        data_provider_page,
                        data_provider,
                        cross_ref_complete_url,
                        data_provider + data_provider_page))

                data_providers.append(data_provider)
                self.logger.info("data provider: %s", data_provider)

        for agm_record in agm_data['data']:
            counter = counter + 1
            global_id = agm_record['primaryID']
            local_id = global_id.split(":")[1]

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(global_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            if agm_record.get('secondaryIds') is not None:
                for sid in agm_record.get('secondaryIds'):
                    agm_secondary_id_dataset = {
                        "primaryId": agm_record.get('primaryID'),
                        "secondaryId": sid
                    }
                    agm_secondary_ids.append(agm_secondary_id_dataset)

            if agm_record.get('synonyms') is not None:
                for syn in agm_record.get('synonyms'):
                    syn_dataset = {
                        "primaryId": agm_record.get('primaryID'),
                        "synonym": syn
                    }
                    agm_synonyms.append(syn_dataset)


            if 'crossReference' in agm_record:
                cross_ref = agm_record.get('crossReference')
                cross_ref_id = cross_ref.get('id')
                local_crossref_id = cross_ref_id.split(":")[1]
                prefix = cross_ref.get('id').split(":")[0]
                pages = cross_ref.get('pages')

                # some pages collection have 0 elements
                if pages is not None and len(pages) > 0:
                    for page in pages:
                        if page in ['Fish', 'genotype', 'strain']:
                            mod_global_cross_ref_url = ETLHelper.get_page_complete_url(local_crossref_id,
                                    self.xref_url_map,
                                    prefix,
                                    page)

            short_species_abbreviation = ETLHelper.get_short_species_abbreviation(agm_record.get('taxonId'))
            name_text = TextProcessingHelper.cleanhtml(agm_record.get('name'))

            # TODO: make subtype required in submission file.

            subtype = agm_record.get('subtype')
            if subtype is None and data_provider == 'WB':
                subtype = 'strain'
            if subtype is None:
                subtype = 'affected_genomic_model'

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

            if agm_record.get('affectedGenomicModelComponents') is not None:

                for component in agm_record.get('affectedGenomicModelComponents'):
                    component_dataset = {
                        "primaryId": agm_record.get('primaryID'),
                        "componentId": component.get('alleleID'),
                        "zygosityId": component.get('zygosity')
                    }
                    components.append(component_dataset)

            if agm_record.get('sequenceTargetingReagentIDs') is not None:
                for sqtr in agm_record.get('sequenceTargetingReagentIDs'):
                    sqtr_dataset = {
                        "primaryId": agm_record.get('primaryID'),
                        "sqtrId": sqtr
                    }
                    sqtrs.append(sqtr_dataset)

            if agm_record.get('parentalPopulationIDs') is not None:
                for background in agm_record.get('parentalPopulationIDs'):
                    background_dataset = {
                        "primaryId": agm_record.get('primaryID'),
                        "backgroundId": background
                    }
                    backgrounds.append(background_dataset)

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
