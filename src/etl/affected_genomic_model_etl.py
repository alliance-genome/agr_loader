import logging, uuid
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)


class AffectedGenomicModelETL(ETL):

    agm_query_template = """
        
    USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (s:Species {primaryKey: row.taxonId})

            //TODO: change the label type based on incoming affectedGenomicModelSpec - for now, these are all
            //genotypes.
            
            MERGE (o:AffectedGenomicModel:Genotype {primaryKey:row.primaryId})
                ON CREATE SET o.name = row.name,
                o.nameText = row.nameText,
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

    agm_secondaryids_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (f:AffectedGenomicModel:Genotype {primaryKey:row.primaryId})

                MERGE (second:SecondaryId:Identifier {primaryKey:row.secondaryId})
                    SET second.name = row.secondary_id
                MERGE (f)-[aka1:ALSO_KNOWN_AS]->(second)

        """

    agm_sqtrs_template = """
        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            
            MATCH (sqtr:SequenceTargetingReagent {primaryKey:row.sqtrId})
            MATCH (agm:AffectedGenomicModel:Genotype {primaryKey:row.primaryId})
            
            MERGE (agm)-[:SEQUENCE_TARGETING_REAGENT]-(sqtr)
    """

    agm_synonyms_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (a:AffectedGenomicModel:Genotype {primaryKey:row.primaryId})

                MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                    SET syn.name = row.synonym
                MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn)

        """

    agm_backgrounds_template = """
     USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            
            MATCH (agm:AffectedGenomicModel:Genotype {primaryKey:row.primaryId})
            MATCH (b:AffectedGenomicModel:Genotype {primaryKey:row.backgroundId})
            
            MERGE (agm)-[:BACKGROUND]-(b)

    """

    agm_components_template = """
     USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (feature:Feature:Allele {primaryKey:row.componentId})
            MATCH (agm:AffectedGenomicModel:Genotype {primaryKey:row.primaryId})
            
            MERGE (agm)-[agmf:MODEL_COMPONENT]-(feature)
                SET agmf.zygosity = row.zygosityId
    
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

        logger.info("Loading Sequence Targeting Reagent Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        logger.info(filepath)
        data = JSONFile().get_data(filepath)
        logger.info("Finished Loading Sequence Targeting Reagent Data: %s" % sub_type.get_data_provider())

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [AffectedGenomicModelETL.agm_query_template,
             commit_size, "agm_data_" + sub_type.get_data_provider() + ".csv"],
            [AffectedGenomicModelETL.agm_secondaryids_template, commit_size,
             "agm_secondaryids_" + sub_type.get_data_provider() + ".csv"],
            [AffectedGenomicModelETL.agm_synonyms_template, commit_size,
             "agm_synonyms_" + sub_type.get_data_provider() + ".csv"],
            [AffectedGenomicModelETL.agm_components_template, commit_size,
             "agm_components_" + sub_type.get_data_provider() + ".csv"],
            [AffectedGenomicModelETL.agm_sqtrs_template, commit_size,
             "agm_sqtrs_" + sub_type.get_data_provider() + ".csv"],
            [AffectedGenomicModelETL.agm_backgrounds_template, commit_size,
             "agm_backgrounds_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)


    def get_generators(self, agm_data, data_provider, batch_size):
        dataProviders = []
        agms = []
        agm_synonyms = []
        agm_secondaryIds = []
        modGlobalCrossRefUrl = ""
        components = []
        backgrounds = []
        sqtrs = []

        counter = 0
        dateProduced = agm_data['metaData']['dateProduced']

        dataProviderObject = agm_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        loadKey = dateProduced + dataProvider + "_agm"

        if dataProviderPages is not None:
            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = ETLHelper.get_page_complete_url(dataProvider, self.xrefUrlMap, dataProvider,
                                                                      dataProviderPage)

                dataProviderCrossRefSet.append(ETLHelper.get_xref_dict(dataProvider, dataProvider, dataProviderPage,
                                                                       dataProviderPage, dataProvider,
                                                                       crossRefCompleteUrl,
                                                                       dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                logger.info("data provider: " + dataProvider)

        for agmRecord in agm_data['data']:
            counter = counter + 1
            globalId = agmRecord['primaryID']
            localId = globalId.split(":")[1]

            if self.testObject.using_test_data() is True:
                is_it_test_entry = self.testObject.check_for_test_id_entry(globalId)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            if agmRecord.get('secondaryIds') is not None:
                for sid in agmRecord.get('secondaryIds'):
                    agm_secondaryId_dataset = {
                        "primaryId": agmRecord.get('primaryID'),
                        "secondaryId": sid
                    }
                    agm_secondaryIds.append(agm_secondaryId_dataset)

            if agmRecord.get('synonyms') is not None:
                for syn in agmRecord.get('synonyms'):
                    syn_dataset = {
                        "primaryId": agmRecord.get('primaryID'),
                        "synonym": syn
                    }
                    agm_synonyms.append(syn_dataset)


            if 'crossReference' in agmRecord:
                crossRef = agmRecord.get('crossReference')
                crossRefId = crossRef.get('id')
                local_crossref_id = crossRefId.split(":")[1]
                prefix = crossRef.get('id').split(":")[0]
                pages = crossRef.get('pages')

                # some pages collection have 0 elements
                if pages is not None and len(pages) > 0:
                    for page in pages:
                        if page == 'sequence_targeting_reagent':
                            modGlobalCrossRefUrl = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                       self.xrefUrlMap, prefix, page)


            # TODO: nameText
            agm_dataset = {
                "primaryId": agmRecord.get('primaryID'),
                "name": agmRecord.get('name'),
                "globalId": globalId,
                "localId": localId,
                "taxonId": agmRecord.get('taxonId'),
                "dataProviders": dataProviders,
                "dateProduced": dateProduced,
                "loadKey": loadKey,
                "modGlobalCrossRefUrl": modGlobalCrossRefUrl,
                "dataProvider": data_provider
            }
            agms.append(agm_dataset)

            if agmRecord.get('affectedGenomicModelComponents') is not None:

                for component in agmRecord.get('affectedGenomicModelComponents'):
                    component_dataset = {
                        "primaryId": agmRecord.get('primaryID'),
                        "componentId": component.get('alleleID'),
                        "zygosityId": component.get('zygosity')
                    }
                    components.append(component_dataset)

            if agmRecord.get('sequenceTargetingReagentIDs') is not None:
                for sqtr in agmRecord.get('sequenceTargetingReagentIDs'):
                    sqtr_dataset = {
                        "primaryId": agmRecord.get('primaryID'),
                        "sqtrId": sqtr
                    }
                    sqtrs.append(sqtr_dataset)

            if agmRecord.get('parentalPopulationIDs') is not None:
                for background in agmRecord.get('parentalPopulationIDs'):
                    background_dataset = {
                        "primaryId": agmRecord.get('primaryID'),
                        "backgroundId": background
                    }
                    backgrounds.append(background_dataset)

            if counter == batch_size:
                yield [agms, agm_secondaryIds, agm_synonyms, components, sqtrs, backgrounds]
                agms = []
                agm_secondaryIds = []
                agm_synonyms = []
                components =[]
                backgrounds = []
                counter = 0

        if counter > 0:
            yield [agms, agm_secondaryIds, agm_synonyms, components, sqtrs, backgrounds]
