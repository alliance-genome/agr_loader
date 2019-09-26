import logging, uuid
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)


class PhenoTypeETL(ETL):

    execute_allele_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (allele:Allele:Feature {primaryKey:row.primaryId})
            
            MATCH (ag:Gene)-[a:IS_ALLELE_OF]-(allele)

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                ON CREATE SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (pa:PhenotypeEntityJoin:Association {primaryKey:row.phenotypeUniqueKey})
                ON CREATE SET 
                    pa.joinType = 'phenotype',
                    pa.dataProviders = row.dataProviders

            MERGE (allele)-[:HAS_PHENOTYPE {uuid:row.phenotypeUniqueKey}]->(p)

            MERGE (allele)-[fpaf:ASSOCIATION]->(pa)
            MERGE (pa)-[pad:ASSOCIATION]->(p)
            MERGE (ag)-[agpa:ASSOCIATION]->(pa)
            
            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl
           
                       MERGE (pubf)-[pe:EVIDENCE]-(pa)
          // CREATE (pubEJ:PublicationEvidenceCodeJoin:Association {primaryKey:row.pecjPrimaryKey})
            // SET pubEJ.joinType = 'pub_evidence_code_join'

           // CREATE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)
            
           // CREATE (pa)-[pubfpubEE:EVIDENCE]->(pubEJ)
            
            """
            
    execute_gene_template = """

            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:Gene {primaryKey:row.primaryId})

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                ON CREATE SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (pa:PhenotypeEntityJoin:Association {primaryKey:row.phenotypeUniqueKey})
                ON CREATE SET 
                    pa.joinType = 'phenotype',
                    pa.dataProviders = row.dataProviders
            
                MERGE (pa)-[pad:ASSOCIATION]->(p)
                MERGE (g)-[gpa:ASSOCIATION]->(pa)
                MERGE (g)-[genep:HAS_PHENOTYPE {uuid:row.phenotypeUniqueKey}]->(p)

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl
           
                       MERGE (pubf)-[pe:EVIDENCE]-(pa)
           // CREATE (pubEJ:PublicationEvidenceCodeJoin:Association {primaryKey:row.pecjPrimaryKey})
               //  SET pubEJ.joinType = 'pub_evidence_code_join'

           // CREATE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)
            
           // CREATE (pa)-[pubfpubEE:EVIDENCE]->(pubEJ) """

    execute_agm_template = """

            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:AffectedGenomicModel {primaryKey:row.primaryId})

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                ON CREATE SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (pa:PhenotypeEntityJoin:Association {primaryKey:row.phenotypeUniqueKey})
                ON CREATE SET 
                    pa.joinType = 'phenotype',
                    pa.dataProviders = row.dataProviders

                MERGE (pa)-[pad:ASSOCIATION]->(p)
                MERGE (g)-[gpa:ASSOCIATION]->(pa)
                MERGE (g)-[genep:HAS_PHENOTYPE {uuid:row.phenotypeUniqueKey}]->(p)

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl
                 
            MERGE (pubf)-[pe:EVIDENCE]-(pa)

          // CREATE (pubEJ:PublicationEvidenceCodeJoin:Association {primaryKey:row.pecjPrimaryKey})
              //  SET pubEJ.joinType = 'pub_evidence_code_join'

          //  CREATE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)
            
          //  CREATE (pa)-[pubfpubEE:EVIDENCE]->(pubEJ) """


    execute_pges_allele_template = """

        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (n:Allele:Feature {primaryKey:row.pgeId})
            MATCH (d:PublicationEvidenceCodeJoin:Association {primaryKey:row.pecjPrimaryKey})

            MERGE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]-(n)

    """

    execute_pges_agm_template = """

        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (n:AffectedGenomicModel {primaryKey:row.pgeId})
            MATCH (d:PublicationEvidenceCodeJoin:Association {primaryKey:row.pecjPrimaryKey})

            MERGE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]-(n)

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
        
        logger.info("Loading Phenotype Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        logger.info("Finished Loading Phenotype Data: %s" % sub_type.get_data_provider())

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = 10000
        
        generators = self.get_generators(data, batch_size)

        query_list = [
            [PhenoTypeETL.execute_gene_template, commit_size, "phenotype_gene_data_" + sub_type.get_data_provider() + ".csv"],
            [PhenoTypeETL.execute_allele_template, commit_size, "phenotype_allele_data_" + sub_type.get_data_provider() + ".csv"],
            [PhenoTypeETL.execute_agm_template, commit_size, "phenotype_agm_data_" + sub_type.get_data_provider() + ".csv"]
            #[PhenoTypeETL.execute_pges_agm_template, commit_size, "phenotype_agm_pge_data_" + sub_type.get_data_provider() + ".csv"] #,
            #[PhenoTypeETL.execute_pges_allele_template, commit_size, "phenotype_agm_allele_data_" + sub_type.get_data_provider() + ".csv"]
        ]
            
        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, phenotype_data, batch_size):
        list_to_yield = []
        pge_list_to_yield = []
        dateProduced = phenotype_data['metaData']['dateProduced']
        dataProviders = []
        dataProviderObject = phenotype_data['metaData']['dataProvider']
        counter = 0
        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []
        pgeKey = ''

        loadKey = dateProduced + dataProvider + "_phenotype"

        if dataProviderPages is not None:
            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = ETLHelper.get_page_complete_url(dataProvider, ETL.xrefUrlMap, dataProvider, dataProviderPage)

                dataProviderCrossRefSet.append(ETLHelper.get_xref_dict(dataProvider, dataProvider, dataProviderPage, dataProviderPage, dataProvider, crossRefCompleteUrl, dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                logger.debug("data provider: " + dataProvider)

        for pheno in phenotype_data['data']:
            pubEntityJoinUuid = str(uuid.uuid4())
            counter = counter + 1
            pubMedId = None
            pubModId = None
            pubMedUrl = None
            pubModUrl = None
            primaryId = pheno.get('objectId')
            phenotypeStatement = pheno.get('phenotypeStatement')

            if self.testObject.using_test_data() is True:
                is_it_test_entry = self.testObject.check_for_test_id_entry(primaryId)
                if is_it_test_entry is False:
                    continue

            evidence = pheno.get('evidence')

            if 'publicationId' in evidence:
                if evidence.get('publicationId').startswith('PMID:'):
                    pubMedId = evidence['publicationId']
                    localPubMedId = pubMedId.split(":")[1]
                    pubMedPrefix = pubMedId.split(":")[0]
                    pubMedUrl = ETLHelper.get_no_page_complete_url(localPubMedId, self.xrefUrlMap, pubMedPrefix,
                                                                   primaryId)
                    if pubMedId is None:
                        pubMedId = ""

                    if 'crossReference' in evidence:
                        pubXref = evidence.get('crossReference')
                        pubModId = pubXref.get('id')
                        pubModLocalId = pubModId.split(":")[1]
                        if pubModId is not None:
                            pubModUrl = ETLHelper.get_complete_pub_url(pubModLocalId, pubModId)

                else:
                    pubModId = evidence.get('publicationId')
                    if pubModId is not None:
                        pubModLocalId = pubModId.split(":")[1]
                        pubModUrl = ETLHelper.get_complete_pub_url(pubModLocalId, pubModId)

                if pubModId is None:
                    pubModId = ""

            if pubMedId is None:
                pubMedId = ""

            if pubModId is None:
                pubModId = ""

            dateAssigned = pheno.get('dateAssigned')

            if pubModId is None and pubMedId is None:
                logger.info (primaryId + "is missing pubMed and pubMod id")

            if 'primaryGeneticEntityIDs' in pheno:
                pgeIds = pheno.get('primaryGeneticEntityIDs')
                for pge in pgeIds:
                    pgeKey = pgeKey + pge
                    pge_map = {"pecjPrimaryKey": pubEntityJoinUuid,
                               "pgeId": pge}
                    pge_list_to_yield.append(pge_map)

            else:
                pgeIds = []

            phenotype = {
                "primaryId":primaryId.strip(),
                "phenotypeUniqueKey": primaryId.strip()+phenotypeStatement.strip(),
                "phenotypeStatement": phenotypeStatement.strip(),
                "dateAssigned": dateAssigned,
                "pubMedId": pubMedId,
                "pubMedUrl": pubMedUrl,
                "pubModId": pubModId,
                "pubModUrl": pubModUrl,
                "pubPrimaryKey": pubMedId + pubModId,
                "loadKey": loadKey,
                "type": "gene",
                "dataProviders": dataProviders,
                "dateProduced": dateProduced,
                "pecjPrimaryKey": pubEntityJoinUuid
             }

            list_to_yield.append(phenotype)

            if counter == batch_size:
                yield [list_to_yield, list_to_yield, list_to_yield] #, pge_list_to_yield]#, pge_list_to_yield, pge_list_to_yield]
                list_to_yield = []
                counter = 0

        if counter > 0:
            yield [list_to_yield, list_to_yield, list_to_yield]# , pge_list_to_yield]#, pge_list_to_yield, pge_list_to_yield]
