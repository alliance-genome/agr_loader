import logging
import multiprocessing

from etl import ETL
from etl.helpers import DiseaseHelper
from etl.helpers import ETLHelper
from etl.helpers import Neo4jHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)

class DiseaseETL(ETL):

    execute_allele_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            // GET PRIMARY DATA OBJECTS

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (allele:Allele:Feature {primaryKey:row.primaryId})
            MATCH (g:Gene)-[a:IS_ALLELE_OF]-(allele)

            //This is an intentional MERGE, please leave as is
            
            MERGE (dfa:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
                ON CREATE SET dfa.dataProvider = row.dataProvider
                ON CREATE SET dfa.sortOrder = 1

            FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                MERGE (allele)<-[faf:IS_MARKER_FOR {uuid:row.diseaseUniqueKey}]->(d)
                SET faf.dateProduced = row.dateProduced,
                 faf.dataProvider = row.dataProvider,
                 dfa.joinType = 'is_marker_of'
            )

            FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                MERGE (allele)<-[faf:IS_IMPLICATED_IN {uuid:row.diseaseUniqueKey}]->(d)
                SET faf.dateProduced = row.dateProduced,
                 faf.dataProvider = row.dataProvider,
                 dfa.joinType = 'is_implicated_in'
            )

            MERGE (allele)-[fdaf:ASSOCIATION]->(dfa)
            MERGE (dfa)-[dadf:ASSOCIATION]->(d)
            MERGE (g)-[gadf:ASSOCIATION]->(dfa)

            // PUBLICATIONS FOR FEATURE
            
            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl
           
            MERGE (pubEJ:PublicationEvidenceCodeJoin:Association {primaryKey:row.uuid})
                ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join'
            
            MERGE (dfa)-[dapug:EVIDENCE {uuid:row.uuid}]->(pubEJ)
            
            MERGE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.uuid}]->(pubEJ)
            """

    execute_gene_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (gene:Gene {primaryKey:row.primaryId})

            MERGE (dga:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
                SET dga.dataProvider = row.dataProvider
                SET dga.sortOrder = 1

            FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                MERGE (gene)<-[fafg:IS_MARKER_FOR {uuid:row.diseaseUniqueKey}]->(d)
                    ON CREATE SET fafg.dataProvider = row.dataProvider,
                                  fafg.dateProduced = row.dateProduced,
                                  dga.joinType = 'is_marker_of')

            FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                MERGE (gene)<-[fafg:IS_IMPLICATED_IN {uuid:row.diseaseUniqueKey}]->(d)
                    ON CREATE SET fafg.dataProvider = row.dataProvider,
                                  fafg.dateProduced = row.dateProduced,
                                  dga.joinType = 'is_implicated_in')

            MERGE (gene)-[fdag:ASSOCIATION]->(dga)
            MERGE (dga)-[dadg:ASSOCIATION]->(d)

            // PUBLICATIONS FOR GENE
    
            MERGE (pubg:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubg.pubModId = row.pubModId,
                    pubg.pubMedId = row.pubMedId,
                    pubg.pubModUrl = row.pubModUrl,
                    pubg.pubMedUrl = row.pubMedUrl
            
            MERGE (pubEJ:PublicationEvidenceCodeJoin:Association {primaryKey:row.uuid})
            ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join'

            MERGE (dga)-[dapug:EVIDENCE]->(pubEJ)
            MERGE (pubg)-[pubgpubEJ:ASSOCIATION {uuid:row.uuid}]->(pubEJ)

            """
    execute_ecode_template = """
    
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Ontology:ECOTerm {primaryKey:row.ecode})
            MATCH (pubjk:PublicationEvidenceCodeJoin {primaryKey:row.uuid})
            MERGE (pubjk)-[daecode1g:ASSOCIATION {uuid:row.uuid}]->(o)
                
    """

    execute_withs_template = """
    
        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (dga:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
        
            MATCH (diseaseWith:Gene {primaryKey:row.withD})
            MERGE (dga)-[dgaw:FROM_ORTHOLOGOUS_GENE]-(diseaseWith)
    
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

        self.delete_empty_nodes()

    def delete_empty_nodes(self):

        logger.debug("delete empty nodes")

        deleteEmptyDONodes = """
                MATCH (dd:DOTerm) WHERE keys(dd)[0] = 'primaryKey' and size(keys(dd)) = 1
                DETACH DELETE (dd)
        """

        Neo4jHelper.run_single_query(deleteEmptyDONodes)

    def _process_sub_type(self, sub_type):
        
        logger.info("Loading Disease Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        logger.info("Finished Loading Disease Data: %s" % sub_type.get_data_provider())

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [DiseaseETL.execute_allele_template, commit_size, "disease_allele_data_" + \
             sub_type.get_data_provider() + ".csv"],
            [DiseaseETL.execute_gene_template, commit_size, "disease_gene_data_" + \
             sub_type.get_data_provider() + ".csv"],
            [DiseaseETL.execute_ecode_template, commit_size, "disease_evidence_code_data_" + \
             sub_type.get_data_provider() + ".csv"],
            [DiseaseETL.execute_withs_template, commit_size, "disease_withs_data_" + \
             sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data, batch_size, sub_type.get_data_provider())
        
        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, disease_data, batch_size, data_provider):
        gene_list_to_yield = []
        allele_list_to_yield = []
        evidence_code_list_to_yield = []
        withs = []
        counter = 0
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
                crossRefCompleteUrl = ETLHelper.get_page_complete_url(dataProvider, ETL.xrefUrlMap,
                                                                      dataProvider, dataProviderPage)

                dataProviderCrossRefSet.append(ETLHelper.get_xref_dict(dataProvider, dataProvider, dataProviderPage,
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
            counter = counter + 1
            diseaseObjectType = diseaseRecord['objectRelation'].get("objectType")

            if diseaseObjectType == "gene":
                disease_record = DiseaseHelper.get_disease_record(diseaseRecord, dataProviders, dateProduced,
                                                                  release, '', data_provider)
                if disease_record is not None:
                    for ecode in disease_record.get('ecodes'):
                        ecode_map = {"uuid": disease_record.get('uuid'),
                                      "ecode": ecode}
                        evidence_code_list_to_yield.append(ecode_map)

                    diseaseUniqueKey = diseaseRecord.get('objectId') + diseaseRecord.get('DOid') + \
                                       diseaseRecord['objectRelation'].get("associationType")

                    if 'with' in diseaseRecord:
                        withRecord = diseaseRecord.get('with')
                        for rec in withRecord:
                            withMap = {
                                "diseaseUniqueKey": diseaseUniqueKey,
                                "withD": rec
                            }
                            withs.append(withMap)

                    gene_list_to_yield.append(disease_record)
                 
            elif diseaseObjectType == "allele":
                disease_record = DiseaseHelper.get_disease_record(diseaseRecord, dataProviders, dateProduced,
                                                                  release, '', data_provider)
                if disease_record is not None:
                    for ecode in disease_record.get('ecodes'):
                        ecode_map = {"uuid": disease_record.get('uuid'),
                                      "ecode": ecode}
                        evidence_code_list_to_yield.append(ecode_map)


                    diseaseUniqueKey = diseaseRecord.get('objectId') + diseaseRecord.get('DOid') + \
                                       diseaseRecord['objectRelation'].get("associationType")

                    if 'with' in diseaseRecord:
                        withRecord = diseaseRecord.get('with')
                        for rec in withRecord:
                            withMap = {
                                "diseaseUniqueKey": diseaseUniqueKey,
                                "withD": rec
                            }
                            withs.append(withMap)

                    allele_list_to_yield.append(disease_record)
            else:
                continue

            if counter == batch_size:
                yield [allele_list_to_yield, gene_list_to_yield, evidence_code_list_to_yield, withs]
                allele_list_to_yield = []
                gene_list_to_yield = []
                evidence_code_list_to_yield = []
                withs = []
                counter = 0

        if counter > 0:
            yield [allele_list_to_yield, gene_list_to_yield, evidence_code_list_to_yield, withs]
