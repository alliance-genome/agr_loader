import logging
logger = logging.getLogger(__name__)

from services import UrlService
from transactors import CSVTransactor
from etl import ETL
from etl.helpers import ETLHelper
from etl.helpers import DiseaseHelper

from files import JSONFile

class DiseaseETL(ETL):

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

    execute_gene_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (gene:Gene {primaryKey:row.primaryId})
            // LOAD NODES

            MERGE (l:Load {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced,
                    l.dataProvider = row.dataProvider,
                    l.loadName = "Disease"

            MERGE (dga:Association:DiseaseEntityJoin {primaryKey:row.uuid})

            FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                MERGE (gene)<-[fafg:IS_MARKER_FOR {uuid:row.uuid}]->(d)
                    SET fafg.dataProvider = row.dataProvider,
                        fafg.dateProduced = row.dateProduced,
                        dga.joinType = 'is_marker_of')

            FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                MERGE (gene)<-[fafg:IS_IMPLICATED_IN {uuid:row.uuid}]->(d)
                    SET fafg.dataProvider = row.dataProvider,
                        fafg.dateProduced = row.dateProduced,
                        dga.joinType = 'is_implicated_in')

            MERGE (gene)-[fdag:ASSOCIATION]->(dga)
            MERGE (dga)-[dadg:ASSOCIATION]->(d)

            // PUBLICATIONS FOR GENE
            MERGE (pubg:Publication {primaryKey:row.pubPrimaryKey})
                SET pubg.pubModId = row.pubModId,
                    pubg.pubMedId = row.pubMedId,
                    pubg.pubModUrl = row.pubModUrl,
                    pubg.pubMedUrl = row.pubMedUrl

            MERGE (l)-[:LOADED_FROM]-(pubg)
            MERGE (dga)-[dapug:EVIDENCE]->(pubg)

            // EVIDENCE CODES FOR GENE
            FOREACH (entity in row.ecodes |
                MERGE (ecode1g:EvidenceCode {primaryKey:entity})
                MERGE (dga)-[daecode1g:EVIDENCE]->(ecode1g)
            )

            """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        for sub_type in self.data_type_config.get_sub_type_objects():
            logger.info("Loading Disease Allele Data: %s" % sub_type.get_data_provider())
            filepath = sub_type.get_filepath()
            data = JSONFile().get_data(filepath)
            logger.info("Finished Loading Disease Allele Data: %s" % sub_type.get_data_provider())

            if data == None:
                logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
                continue

            commit_size = self.data_type_config.get_neo4j_commit_size()
            batch_size = self.data_type_config.get_generator_batch_size()

            # This needs to be in this format (template, param1, params2) others will be ignored
            query_list = [
                [DiseaseETL.execute_feature_template, commit_size, "disease_allele_data_" + sub_type.get_data_provider() + ".csv"],
                [DiseaseETL.execute_gene_template, commit_size, "disease_gene_data_" + sub_type.get_data_provider() + ".csv"],
            ]

            # Obtain the generator
            generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)
            
            # Prepare the transaction
            CSVTransactor.execute_transaction(generators, query_list)

    def get_generators(self, disease_data, batch_size, data_provider):
        list_to_yield = []
        dateProduced = disease_data['metaData']['dateProduced']

        dataProviders = []

        dataProviderObject = disease_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        #TODO: get SGD to fix their files.

        if dataProviderPages is not None:
            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, ETL.xrefUrlMap, dataProvider, dataProviderPage)

                dataProviderCrossRefSet.append(ETLHelper.get_xref_dict(dataProvider, dataProvider,
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

                disease_features = DiseaseHelper.get_disease_record(diseaseRecord, dataProviders, dateProduced, release, allelicGeneId, data_provider)

                list_to_yield.append(disease_features)
                if len(list_to_yield) == batch_size:
                    yield [list_to_yield, list_to_yield]
                    list_to_yield = []

        if len(list_to_yield) > 0:
            yield [list_to_yield, list_to_yield]
