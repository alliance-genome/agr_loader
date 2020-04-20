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
        publication_mod_id = ""
        pub_med_id = ""
        pub_mod_url = None
        pub_med_url = None
        pge_key = ''
        disease_association_type = None
        pecj_primary_key = str(uuid.uuid4())

        data_provider_object = disease_data['metaData']['dataProvider']

        data_provider_cross_ref = data_provider_object.get('crossReference')
        data_provider = data_provider_cross_ref.get('id')


        for disease_record in disease_data['data']:

            if self.testObject.using_test_data() is True:
                is_it_test_entry = self.testObject.check_for_test_id_entry(disease_record.get('objectId'))
                if is_it_test_entry is False:
                    continue

            disease_unique_key = disease_record.get('objectId') + disease_record.get('DOid') + \
                               disease_record['objectRelation'].get("associationType").upper()

            counter = counter + 1
            disease_object_type = disease_record['objectRelation'].get("objectType")

            primary_id = disease_record.get('objectId')
            do_id = disease_record.get('DOid')


            if 'qualifier' in disease_record:
                qualifier = disease_record.get('qualifier')

            else:

                if 'evidence' in disease_record:
                    if primary_id == 'HGNC:11950':
                        logger.info("evidence is in disease record")
                    evidence = disease_record.get('evidence')
                    if 'publication' in evidence:
                        publication = evidence.get('publication')
                        if publication.get('publicationId').startswith('PMID:'):
                            pub_med_id = publication.get('publicationId')
                            local_pub_med_id = pub_med_id.split(":")[1]
                            pub_med_url = ETLHelper.get_complete_pub_url(local_pub_med_id, pub_med_id)
                            if 'crossReference' in evidence:
                                pub_xref = evidence.get('crossReference')
                                publication_mod_id = pub_xref.get('id')
                                local_pub_mod_id = publication_mod_id.split(":")[1]
                                pub_mod_url = ETLHelper.get_complete_pub_url(local_pub_mod_id, publication_mod_id)
                        else:
                            publication_mod_id = publication.get('publicationId')
                            local_pub_mod_id = publication_mod_id.split(":")[1]
                            pub_mod_url = ETLHelper.get_complete_pub_url(local_pub_mod_id, publication_mod_id)

                if 'objectRelation' in disease_record:
                    disease_association_type = disease_record['objectRelation'].get("associationType").upper()

                    additional_genetic_components = []

                    if 'additionalGeneticComponents' in disease_record['objectRelation']:
                        for component in disease_record['objectRelation']['additionalGeneticComponents']:
                            component_symbol = component.get('componentSymbol')
                            component_id = component.get('componentId')
                            component_url = component.get('componentUrl') + component_id
                            additional_genetic_components.append(
                                {"id": component_id, "componentUrl": component_url, "componentSymbol": component_symbol}
                            )

                if 'evidenceCodes' in disease_record['evidence']:
                    for ecode in disease_record['evidence'].get('evidenceCodes'):
                        ecode_map = {"pecjPrimaryKey": pecj_primary_key,
                                     "ecode": ecode}
                        evidence_code_list_to_yield.append(ecode_map)

                if 'with' in disease_record:
                    with_record = disease_record.get('with')
                    for rec in with_record:
                        disease_unique_key = disease_unique_key + rec
                    for rec in with_record:
                        with_map = {
                            "diseaseUniqueKey": disease_unique_key,
                            "withD": rec
                        }
                        withs.append(with_map)

                if 'primaryGeneticEntityIDs' in disease_record:

                    pge_ids = disease_record.get('primaryGeneticEntityIDs')

                    for pge in pge_ids:
                        pge_key = pge_key + pge
                        pge_map = {"pecjPrimaryKey": pecj_primary_key,
                                   "pgeId": pge}
                        pge_list_to_yield.append(pge_map)

                if 'dataProvider' in disease_record:
                    for dp in disease_record['dataProvider']:
                        annotation_type = dp.get('type')
                        xref = dp.get('crossReference')
                        cross_ref_id = xref.get('id')
                        pages = xref.get('pages')

                        local_crossref_id = ""
                        prefix = cross_ref_id

                        if annotation_type is None:
                            annotation_type = 'curated'

                        if pages is not None and len(pages) > 0:
                            for page in pages:
                                # TODO: get the DQMs to restructure this so that we get a global id here instead of
                                # RGD
                                if page == 'homepage' and cross_ref_id == 'RGD':

                                    local_crossref_id = primary_id.split(":")[1]
                                    cross_ref_id = primary_id
                                    prefix = "RGD"

                                    page = 'disease/rat'
                                    mod_global_cross_ref_id = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                              self.xrefUrlMap, prefix,
                                                                                              page)
                                    passing_xref = ETLHelper.get_xref_dict(local_crossref_id, prefix, page, page,
                                                                           cross_ref_id,
                                                                           mod_global_cross_ref_id,
                                                                           cross_ref_id + page + annotation_type)
                                    passing_xref['dataId'] = disease_unique_key
                                else:
                                    mod_global_cross_ref_id = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                              self.xrefUrlMap, prefix,
                                                                                              page)
                                    passing_xref = ETLHelper.get_xref_dict(local_crossref_id, prefix, page, page,
                                                                           cross_ref_id,
                                                                           mod_global_cross_ref_id,
                                                                           cross_ref_id + page + annotation_type)
                                    passing_xref['dataId'] = disease_unique_key

                                if 'loaded' in annotation_type:
                                    passing_xref['loadedDB'] = 'true'
                                    passing_xref['curatedDB'] = 'false'
                                else:
                                    passing_xref['curatedDB'] = 'true'
                                    passing_xref['loadedDB'] = 'false'
                                xrefs.append(passing_xref)

                disease_record = {"diseaseUniqueKey": disease_unique_key,
                                  "doId": do_id,
                                  "primaryId": primary_id,
                                  "pecjPrimaryKey": pecj_primary_key,
                                  "relationshipType": disease_association_type.upper(),
                                  "dataProvider": data_provider,
                                  "dateAssigned": disease_record.get("dateAssigned"),
                                  "pubPrimaryKey": publication_mod_id + pub_med_id,
                                  "pubModId": publication_mod_id,
                                  "pubMedId": pub_med_id,
                                  "pubMedUrl": pub_med_url,
                                  "pubModUrl": pub_mod_url}

                logger.info(disease_record)
                if disease_object_type == 'gene':
                    gene_list_to_yield.append(disease_record)
                elif disease_object_type == 'allele':
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
