from etl import ETL
import logging
from transactors import *
from services import *
from etl.helpers import *
from extractors import ResourceDescriptorExtractor

logger = logging.getLogger(__name__)

class DiseaseAlleleETL(ETL):

    execute_feature_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            // GET PRIMARY DATA OBJECTS

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (feature:Feature {primaryKey:row.primaryId})
            MATCH (g:Gene)-[a:IS_ALLELE_OF]-(feature)

            // LOAD NODES
            MERGE (l:Load:Entity {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced,
                 l.loadName = "Disease",
                 l.dataProviders = row.dataProviders,
                 l.dataProvider = row.dataProvider


            MERGE (dfa:Association:DiseaseEntityJoin {primaryKey:row.uuid})
                SET dfa.dataProviders = row.dataProviders
                
            MERGE (dfa)-[dfal:LOADED_FROM]-(l)

            FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                MERGE (feature)<-[faf:IS_MARKER_FOR {uuid:row.uuid}]->(d)
                SET faf.dateProduced = row.dateProduced,
                 faf.dataProvider = row.dataProvider,
                 dfa.joinType = 'is_marker_of'
            )

            FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                MERGE (feature)<-[faf:IS_IMPLICATED_IN {uuid:row.uuid}]->(d)
                SET faf.dateProduced = row.dateProduced,
                 faf.dataProvider = row.dataProvider,
                 dfa.joinType = 'is_implicated_in'
            )

            //FOREACH (dataProvider in row.dataProviders |
                //MERGE (dp:DataProvider {primaryKey:dataProvider})
                //MERGE (dfa)-[odp:DATA_PROVIDER]-(dp)
                //MERGE (l)-[ldp:DATA_PROVIDER]-(dp))


            MERGE (feature)-[fdaf:ASSOCIATION]->(dfa)
            MERGE (dfa)-[dadf:ASSOCIATION]->(d)
            MERGE (g)-[gadf:ASSOCIATION]->(dfa)

            // PUBLICATIONS FOR FEATURE
            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl

            MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
            MERGE (dfa)-[dapuf:EVIDENCE]->(pubf)

            // EVIDENCE CODES FOR FEATURE
            FOREACH (entity in row.ecodes|
                MERGE (ecode1f:EvidenceCode {primaryKey:entity})
                MERGE (dfa)-[daecode1f:EVIDENCE]->(ecode1f)
            ) """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        for mod_config in self.data_type_config.get_mod_configs():
            logger.info("Loading Disease Allele Data: %s" % mod_config.data_provider)
            data = mod_config.get_disease_data()
            logger.info("Finished Loading Disease Allele Data: %s" % mod_config.data_provider)

            if data == None:
                logger.warn("No Data found for %s skipping" % mod_config.data_provider)
                continue

            commit_size = self.data_type_config.get_neo4j_commit_size()
            batch_size = self.data_type_config.get_generator_batch_size()

            # This needs to be in this format (template, param1, params2) others will be ignored
            query_list = [
                [DiseaseAlleleETL.execute_feature_template, commit_size, "disease_allele_data_" + mod_config.data_provider + ".csv"],
            ]

            # Obtain the generator
            dataset = self.get_generators(data, mod_config.data_provider, batch_size)

            # Prepare the transaction
            CSVTransactor.execute_transaction(dataset, query_list)

    def get_generators(self, disease_data, batch_size, data_provider):
        list_to_yield = []
        dateProduced = disease_data['metaData']['dateProduced']

        xrefUrlMap = ResourceDescriptorExtractor().get_data()
        dataProviders = []

        dataProviderObject = disease_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        #TODO: get SGD to fix their files.

        if dataProviderPages is not None:
            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, xrefUrlMap, dataProvider,
                                                                       dataProviderPage)

                dataProviderCrossRefSet.append(ETL.get_xref_dict(dataProvider, dataProvider,
                                                                             dataProviderPage,
                                                                             dataProviderPage, dataProvider,
                                                                             crossRefCompleteUrl,
                                                                             dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                logger.info("data provider: " + dataProvider)

        if 'release' in disease_data['metaData']:
            release = disease_data['metaData']['release']
        else:
            release = ''

        for diseaseRecord in disease_data['data']:

            diseaseObjectType = diseaseRecord['objectRelation'].get("objectType")

            if diseaseObjectType != "allele":
                     continue
            else:
                # query = "match (g:Gene)-[]-(f:Feature) where f.primaryKey = {parameter} return g.primaryKey"
                # featurePrimaryId = diseaseRecord.get('objectId')
                # returnSet = Transaction.run_single_parameter_query(query, featurePrimaryId)
                # counter = 0
                # allelicGeneId = ''
                # for gene in returnSet:
                #     counter += 1
                #     allelicGeneId = gene["g.primaryKey"]
                # if counter > 1:
                #     allelicGeneId = ''
                #     logger.info ("returning more than one gene: this is an error")
                allelicGeneId = ''

                disease_features = DiseaseAlleleHelper.get_disease_record(diseaseRecord, dataProviders, dateProduced, release, allelicGeneId, data_provider)

                list_to_yield.append(disease_features)
                if len(list_to_yield) == batch_size:
                    yield [list_to_yield]
                    list_to_yield = []

        if len(list_to_yield) > 0:
            yield [list_to_yield]
