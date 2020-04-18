import logging
import multiprocessing
import uuid
from etl import ETL
from etl.helpers import ETLHelper
from etl.helpers import Neo4jHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)

class DiseaseETL(ETL):


    execute_annotation_xrefs_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:DiseaseEntityJoin {primaryKey:row.dataId}) """ + ETLHelper.get_cypher_xref_text_annotation_level()

    execute_agms_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            // GET PRIMARY DATA OBJECTS

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (agm:AffectedGenomicModel {primaryKey:row.primaryId})
            
            CALL apoc.create.relationship(d, row.relationshipType, {}, agm) yield rel
            SET rel.uuid = row.diseaseUniqueKey
            REMOVE rel.noOp
            
            //This is an intentional MERGE, please leave as is

            MERGE (dfa:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
                ON CREATE SET dfa.dataProvider = row.dataProvider,
                              dfa.sortOrder = 1,
                              dfa.joinType = row.relationshipType
                              
            MERGE (agm)-[fdaf:ASSOCIATION]->(dfa)
            MERGE (dfa)-[dadf:ASSOCIATION]->(d)

            // PUBLICATIONS FOR FEATURE

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl

            MERGE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
                ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join',
                                pubEJ.dateAssigned = row.dateAssigned

            MERGE (dfa)-[dapug:EVIDENCE {uuid:row.pecjPrimaryKey}]->(pubEJ)

            MERGE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)
            """

    execute_allele_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            // GET PRIMARY DATA OBJECTS

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (allele:Allele:Feature {primaryKey:row.primaryId})
            MATCH (g:Gene)-[a:IS_ALLELE_OF]-(allele)
            
            CALL apoc.create.relationship(d, row.relationshipType, {}, allele) yield rel
                        SET rel.uuid = row.diseaseUniqueKey 
            REMOVE rel.noOp
            
            //This is an intentional MERGE, please leave as is
            
            MERGE (dfa:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
                ON CREATE SET dfa.dataProvider = row.dataProvider,
                              dfa.sortOrder = 1,
                              dfa.joinType = row.relationshipType


            
            MERGE (allele)-[fdaf:ASSOCIATION]->(dfa)
            MERGE (dfa)-[dadf:ASSOCIATION]->(d)
            MERGE (g)-[dag:ASSOCIATION]->(d)
            
            // PUBLICATIONS FOR FEATURE
            
            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl
           
            MERGE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
                ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join',
                                pubEJ.dateAssigned = row.dateAssigned
            
            MERGE (dfa)-[dapug:EVIDENCE {uuid:row.pecjPrimaryKey}]->(pubEJ)
            
            MERGE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)
            """

    execute_gene_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (gene:Gene {primaryKey:row.primaryId})
            
            CALL apoc.create.relationship(d, row.relationshipType, {}, gene) yield rel
                        SET rel.uuid = row.diseaseUniqueKey 
            REMOVE rel.noOp
            
            MERGE (dga:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
                SET dga.dataProvider = row.dataProvider,
                    dga.sortOrder = 1,
                    dga.joinType = row.relationshipType


            MERGE (gene)-[fdag:ASSOCIATION]->(dga)
            MERGE (dga)-[dadg:ASSOCIATION]->(d)

            // PUBLICATIONS FOR GENE
    
            MERGE (pubg:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubg.pubModId = row.pubModId,
                    pubg.pubMedId = row.pubMedId,
                    pubg.pubModUrl = row.pubModUrl,
                    pubg.pubMedUrl = row.pubMedUrl
            
            MERGE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
            ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join',
                                pubEJ.dateAssigned = row.dateAssigned

            MERGE (dga)-[dapug:EVIDENCE {uuid:row.pecjPrimaryKey}]->(pubEJ)
            MERGE (pubg)-[pubgpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)

            """
    execute_ecode_template = """
    
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Ontology:ECOTerm {primaryKey:row.ecode})
            MATCH (pubjk:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
            MERGE (pubjk)-[daecode1g:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(o)
                
    """

    execute_withs_template = """
    
        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (dga:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
        
            MATCH (diseaseWith:Gene {primaryKey:row.withD})
            MERGE (dga)-[dgaw:FROM_ORTHOLOGOUS_GENE]-(diseaseWith)
    
    """

    execute_pges_gene_template = """

        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (n:Gene {primaryKey:row.pgeId})
            MATCH (d:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
            
            MERGE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]-(n)

    """

    execute_pges_allele_template = """

        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (n:Allele {primaryKey:row.pgeId})
            MATCH (d:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})

            MERGE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]-(n)

    """

    execute_pges_agm_template = """

        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (n:AffectedGenomicModel {primaryKey:row.pgeId})
            MATCH (d:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})

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
            [DiseaseETL.execute_agms_template, commit_size, "disease_agms_data_" + \
             sub_type.get_data_provider() + ".csv"],
            [DiseaseETL.execute_pges_gene_template, commit_size, "disease_pges_gene_data_" + \
             sub_type.get_data_provider() + ".csv"],
            [DiseaseETL.execute_pges_allele_template, commit_size, "disease_pges_allele_data_" + \
             sub_type.get_data_provider() + ".csv"],
            [DiseaseETL.execute_pges_agm_template, commit_size, "disease_pges_agms_data_" + \
             sub_type.get_data_provider() + ".csv"],
            [DiseaseETL.execute_withs_template, commit_size, "disease_withs_data_" + \
             sub_type.get_data_provider() + ".csv"],
            [DiseaseETL.execute_ecode_template, commit_size, "disease_evidence_code_data_" + \
             sub_type.get_data_provider() + ".csv"],
            [DiseaseETL.execute_annotation_xrefs_template, commit_size, "disease_annotation_xrefs_data_" + \
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
        agm_list_to_yield = []
        evidence_code_list_to_yield = []
        withs =[]
        pge_list_to_yield = []
        xrefs = []
        counter = 0
        dateProduced = disease_data['metaData']['dateProduced']
        publicationModId = ""
        pubMedId = ""
        pubModUrl = None
        pubMedUrl = None
        diseaseAssociationType = None
        ecodes = []
        annotationDP = []
        pgeIds = []
        annotationUuid = str(uuid.uuid4())
        pecjPrimaryKey = annotationUuid
        dataProviders = []

        dataProviderObject = disease_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        qualifier = None
        pgeKey = ''

        for diseaseRecord in disease_data['data']:

            diseaseUniqueKey = diseaseRecord.get('objectId') + diseaseRecord.get('DOid') + \
                               diseaseRecord['objectRelation'].get("associationType").upper()

            counter = counter + 1
            diseaseObjectType = diseaseRecord['objectRelation'].get("objectType")

            primaryId = diseaseRecord.get('objectId')
            doId = diseaseRecord.get('DOid')

            loadKey = dateProduced + "_Disease"

            for dataProvider in dataProviders:
                loadKey = dataProvider + loadKey

            if 'qualifier' in diseaseRecord:
                qualifier = diseaseRecord.get('qualifier')

            if qualifier is None:
                if 'evidence' in diseaseRecord:

                    evidence = diseaseRecord.get('evidence')
                    if 'publication' in evidence:
                        publication = evidence.get('publication')
                        if publication.get('publicationId').startswith('PMID:'):
                            pubMedId = publication.get('publicationId')
                            localPubMedId = pubMedId.split(":")[1]
                            pubMedUrl = ETLHelper.get_complete_pub_url(localPubMedId, pubMedId)
                            if 'crossReference' in evidence:
                                pubXref = evidence.get('crossReference')
                                publicationModId = pubXref.get('id')
                                localPubModId = publicationModId.split(":")[1]
                                pubModUrl = ETLHelper.get_complete_pub_url(localPubModId, publicationModId)
                        else:
                            publicationModId = publication.get('publicationId')
                            localPubModId = publicationModId.split(":")[1]
                            pubModUrl = ETLHelper.get_complete_pub_url(localPubModId, publicationModId)

                if 'objectRelation' in diseaseRecord:
                    diseaseAssociationType = diseaseRecord['objectRelation'].get("associationType").upper()

                    additionalGeneticComponents = []
                    if 'additionalGeneticComponents' in diseaseRecord['objectRelation']:
                        for component in diseaseRecord['objectRelation']['additionalGeneticComponents']:
                            componentSymbol = component.get('componentSymbol')
                            componentId = component.get('componentId')
                            componentUrl = component.get('componentUrl') + componentId
                            additionalGeneticComponents.append(
                                {"id": componentId, "componentUrl": componentUrl, "componentSymbol": componentSymbol}
                            )

                if 'with' in diseaseRecord:
                    withRecord = diseaseRecord.get('with')
                    for rec in withRecord:
                        diseaseUniqueKey = diseaseUniqueKey + rec

                else:
                    pgeIds = []


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

                if 'evidenceCodes' in diseaseRecord['evidence']:
                    ecodes = diseaseRecord['evidence'].get('evidenceCodes')

                for ecode in ecodes:
                    ecode_map = {"pecjPrimaryKey": pecjPrimaryKey,
                                     "ecode": ecode}
                    evidence_code_list_to_yield.append(ecode_map)


                if 'with' in diseaseRecord:
                    withRecord = diseaseRecord.get('with')
                    for rec in withRecord:
                        diseaseUniqueKey = diseaseUniqueKey+rec
                    for rec in withRecord:
                        withMap = {
                                "diseaseUniqueKey": diseaseUniqueKey,
                                "withD": rec
                        }
                        withs.append(withMap)

                if 'primaryGeneticEntityIDs' in diseaseRecord:

                    pgeIds = diseaseRecord.get('primaryGeneticEntityIDs')

                    for pge in pgeIds:
                        pgeKey = pgeKey + pge
                        pge_map = {"pecjPrimaryKey": pecjPrimaryKey,
                                       "pgeId": pge}
                        pge_list_to_yield.append(pge_map)


                if 'dataProvider' in diseaseRecord:
                    for dp in diseaseRecord['dataProvider']:
                        annotationType = dp.get('type')
                        xref = dp.get('crossReference')
                        crossRefId = xref.get('id')
                        pages = xref.get('pages')

                        local_crossref_id = ""
                        prefix = crossRefId

                        if annotationType is None:
                            annotationType = 'curated'

                        if pages is not None and len(pages) > 0:
                            for page in pages:
                                # TODO: get the DQMs to restructure this so that we get a global id here instead of
                                # RGD
                                if page == 'homepage' and crossRefId == 'RGD':

                                    local_crossref_id = primaryId.split(":")[1]
                                    crossRefId = primaryId
                                    prefix = "RGD"

                                    page = 'disease/rat'
                                    modGlobalCrossRefId = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                             self.xrefUrlMap, prefix,
                                                                                              page)
                                    passing_xref = ETLHelper.get_xref_dict(local_crossref_id, prefix, page, page,
                                                                       crossRefId,
                                                                       modGlobalCrossRefId,
                                                                       crossRefId + page + annotationType)
                                    passing_xref['dataId'] = diseaseUniqueKey
                                else:
                                    modGlobalCrossRefId = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                          self.xrefUrlMap, prefix, page)
                                    passing_xref = ETLHelper.get_xref_dict(local_crossref_id, prefix, page, page, crossRefId,
                                                                   modGlobalCrossRefId,
                                                                   crossRefId + page + annotationType)
                                    passing_xref['dataId'] = diseaseUniqueKey

                                if 'loaded' in annotationType:
                                    passing_xref['loadedDB'] = 'true'
                                    passing_xref['curatedDB'] = 'false'
                                else:
                                    passing_xref['curatedDB'] = 'true'
                                    passing_xref['loadedDB'] = 'false'

                                xrefs.append(passing_xref)

                disease_record = {"diseaseUniqueKey": diseaseUniqueKey,
                                  "doId": doId,
                                  "primaryId": primaryId,
                                  "pecjPrimaryKey": annotationUuid,
                                  "dataProviders": dataProviders,
                                  "relationshipType": diseaseAssociationType.upper(),
                                  "dateProduced": dateProduced,
                                  "dataProvider": data_provider,
                                  "dateAssigned": diseaseRecord.get("dateAssigned"),

                                  "pubPrimaryKey": publicationModId + pubMedId,

                                  "pubModId": publicationModId,
                                  "pubMedId": pubMedId,
                                  "pubMedUrl": pubMedUrl,
                                  "pubModUrl": pubModUrl,
                                  "pgeIds": pgeIds,
                                  "pgeKey": pgeKey,
                                  "annotationDP": annotationDP,
                                  "ecodes": ecodes
                                  }



                if diseaseObjectType == 'gene':
                    gene_list_to_yield.append(disease_record)
                elif diseaseObjectType == 'allele':
                    allele_list_to_yield.append(disease_record)
                else:
                    agm_list_to_yield.append(disease_record)

            if counter == batch_size:
                yield [allele_list_to_yield, gene_list_to_yield,
                       agm_list_to_yield, pge_list_to_yield, pge_list_to_yield, pge_list_to_yield,
                       withs, evidence_code_list_to_yield, xrefs]
                agm_list_to_yield = []
                allele_list_to_yield = []
                gene_list_to_yield = []
                evidence_code_list_to_yield = []
                pge_list_to_yield = []
                xrefs = []
                withs = []
                counter = 0

        if counter > 0:
            yield [allele_list_to_yield, gene_list_to_yield,
                   agm_list_to_yield, pge_list_to_yield, pge_list_to_yield, pge_list_to_yield,
                       withs, evidence_code_list_to_yield, xrefs]

