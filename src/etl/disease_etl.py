'''Disease ETL'''

##TODO need to fix the difference between disaeseRecord and disease_record in original code

import logging
import multiprocessing

from etl import ETL
from etl.helpers import DiseaseHelper
from etl.helpers import ETLHelper
from etl.helpers import Neo4jHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor


class DiseaseETL(ETL):
    '''Disease ETL'''

    logger = logging.getLogger(__name__)

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
                              dfa.dateAssigned = row.dateAssigned,
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
                ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join'

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
                              dfa.dateAssigned = row.dateAssigned,
                              dfa.sortOrder = 1,
                              dfa.joinType = row.relationshipType


            
            MERGE (allele)-[fdaf:ASSOCIATION]->(dfa)
            MERGE (dfa)-[dadf:ASSOCIATION]->(d)
            MERGE (g)-[gadf:ASSOCIATION]->(dfa)

            // PUBLICATIONS FOR FEATURE
            
            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl
           
            MERGE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
                ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join'
            
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
                    dga.dateAssigned = row.dateAssigned,
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
            ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join'

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
            process = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

        self.delete_empty_nodes()

    def delete_empty_nodes(self):

        self.logger.debug("delete empty nodes")

        delete_empty_do_nodes = """
                MATCH (dd:DOTerm) WHERE keys(dd)[0] = 'primaryKey' and size(keys(dd)) = 1
                DETACH DELETE (dd)
        """

        Neo4jHelper.run_single_query(delete_empty_do_nodes)

    def _process_sub_type(self, sub_type):
        
        self.logger.info("Loading Disease Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        self.logger.info("Finished Loading Disease Data: %s", sub_type.get_data_provider())

        if data is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
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

    # TODO: get this method working instead of repeating code below -- have to make generator work within a generator.
    def get_disease_details(self, disease_record, diseaseRecord):

        if disease_record is not None:
            for ecode in disease_record.get('ecodes'):
                ecode_map = {"uuid": disease_record.get('uuid'),
                             "ecode": ecode}
                self.evidence_code_list_to_yield.append(ecode_map)

            disease_unique_key = disease_record.get('objectId') + \
                                 disease_record.get('DOid') + \
                                 disease_record['objectRelation'].get("associationType").upper()

            if disease_record.get('pgeIds') is not None:
                for pge in disease_record.get('pgeIds'):
                    pge_map = {"dgeId": disease_unique_key,
                               "pgeId": pge}
         
                    self.pge_list_to_yield.append(pge_map)

            if 'with' in diseaseRecord:
                with_record = disease_record.get('with')
                for rec in with_record:
                    with_map = {
                        "diseaseUniqueKey": disease_unique_key,
                        "withD": rec
                    }
                    self.withs.append(with_map)

            if 'annotationDP' in disease_record:
                for adp in disease_record['annotationDP']:
                    cross_ref_id = adp.get('crossRefId')
                    pages = adp.get('dpPages')
                    annotation_type = adp.get('annotationType')
                    local_crossref_id = ""
                    prefix = cross_ref_id
                    if pages is not None and len(pages) > 0:
                        for page in pages:
                            modGlobalCrossRefId = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                  self.xrefUrlMap,
                                                                                  prefix,
                                                                                  page)
                            xref = ETLHelper.get_xref_dict(local_crossref_id,
                                                           prefix,
                                                           page,
                                                           page,
                                                           cross_ref_id,
                                                           mod_global_cross_ref_id,
                                                           cross_ref_id + page)
                            xref['dataId'] = disease_unique_key
                            if annotation_type == 'Loaded':
                                xref['loadedDB'] = cross_ref_id
                            else:
                                xref['curatedDB'] = cross_ref_id

                            self.xrefs.append(xref)

    def get_generators(self, disease_data, batch_size, data_provider):
        '''Creating generators'''

        gene_list_to_yield = []
        allele_list_to_yield = []
        evidence_code_list_to_yield = []
        agm_list_to_yield = []
        withs = []
        pge_list_to_yield = []
        xrefs = []
        counter = 0
        date_produced = disease_data['metaData']['dateProduced']

        data_providers = []

        data_provider_object = disease_data['metaData']['dataProvider']

        data_provider_cross_ref = data_provider_object.get('crossReference')
        data_provider = data_provider_cross_ref.get('id')
        data_provider_pages = data_provider_cross_ref.get('pages')
        data_provider_cross_ref_set = []

        #TODO: get SGD to fix their files.
        if data_provider_pages is not None:
            for data_provider_page in data_provider_pages:
                cross_ref_complete_url = ETLHelper.get_page_complete_url(data_provider,
                                                                         ETL.xref_url_map,
                                                                         data_provider,
                                                                         data_provider_page)

                data_provider_cross_ref_set.append(ETLHelper.get_xref_dict(data_provider,
                                                                           data_provider,
                                                                           data_provider_page,
                                                                           data_provider_page,
                                                                           data_provider,
                                                                           cross_ref_complete_url,
                                                                           data_provider + data_provider_page))

                data_providers.append(data_provider)
                self.logger.info("data provider: %s", data_provider)

        if 'release' in disease_data['metaData']:
            release = disease_data['metaData']['release']
        else:
            release = ''

        for disease_record in disease_data['data']:
            counter = counter + 1
            disease_object_type = disease_record['objectRelation'].get("objectType")

            if disease_object_type == "gene":
                disease_record = DiseaseHelper.get_disease_record(disease_record,
                                                                  data_providers,
                                                                  date_produced,
                                                                  data_provider)

                if disease_record is not None:
                    pecj_primary_key = disease_record.get('pecjPrimaryKey')


                    for ecode in disease_record.get('ecodes'):
                        ecode_map = {"pecjPrimaryKey": pecj_primary_key,
                                     "ecode": ecode}
                        evidence_code_list_to_yield.append(ecode_map)

                    disease_unique_key = disease_record.get('objectId') + disease_record.get('DOid') + \
                                         disease_record['objectRelation'].get("associationType").upper()


                    if disease_record.get('pgeIds') is not None:
                        for pge in disease_record.get('pgeIds'):
                            pge_map = {"pecjPrimaryKey": pecj_primary_key,
                                       "pgeId": pge}
                            pge_list_to_yield.append(pge_map)

                    if 'with' in disease_record:
                        with_record = disease_record.get('with')
                        for rec in with_record:
                            with_map = {
                                "diseaseUniqueKey": disease_unique_key,
                                "withD": rec
                            }
                            withs.append(with_map)

                    if 'annotationDP' in disease_record:
                        for adp in disease_record['annotationDP']:
                            cross_ref_id = adp.get('crossRefId')
                            pages = adp.get('dpPages')
                            annotation_type = adp.get('annotationType')

                            local_crossref_id = ""
                            prefix = cross_ref_id
                            if annotation_type is None:
                                annotation_type = 'curated'
                            if pages is not None and len(pages) > 0:
                                for page in pages:
                                    mod_global_cross_ref_id = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                              self.xrefUrlMap,
                                                                                              prefix,
                                                                                              page)
                                    xref = ETLHelper.get_xref_dict(local_crossref_id,
                                                                   prefix,
                                                                   page,
                                                                   page,
                                                                   cross_ref_id,
                                                                   mod_global_cross_ref_id,
                                                                   cross_ref_id + page + annotation_type)
                                    xref['dataId'] = disease_unique_key
                                    if 'loaded' in annotation_type:
                                        xref['loadedDB'] = 'true'
                                        xref['curatedDB'] = 'false'
                                    else:
                                        xref['curatedDB'] = 'true'
                                        xref['loadedDB'] = 'false'

                                    xrefs.append(xref)
                gene_list_to_yield.append(disease_record)
                 
            elif disease_object_type == "allele":
                disease_record = DiseaseHelper.get_disease_record(disease_record,
                                                                  data_providers,
                                                                  date_produced,
                                                                  data_provider)

                if disease_record is not None:

                    pecj_primary_key = disease_record.get('pecjPrimaryKey')


                    for ecode in disease_record.get('ecodes'):
                        ecode_map = {"pecjPrimaryKey": pecj_primary_key,
                                     "ecode": ecode}
                        evidence_code_list_to_yield.append(ecode_map)

                    disease_unique_key = disease_record.get('objectId') + \
                                         disease_record.get('DOid') + \
                                         disease_record['objectRelation'].get("associationType").upper()

                    if disease_record.get('pgeIds') is not None:
                        for pge in disease_record.get('pgeIds'):
                            pge_map = {"pecjPrimaryKey": pecj_primary_key,
                                       "pgeId": pge}
                            pge_list_to_yield.append(pge_map)

                    if 'with' in disease_record:
                        with_record = disease_record.get('with')
                        for rec in with_record:
                            with_map = {
                                "diseaseUniqueKey": disease_unique_key,
                                "withD": rec
                            }
                            withs.append(with_map)

                    if 'annotationDP' in disease_record:
                        for adp in disease_record['annotationDP']:
                            cross_ref_id = adp.get('crossRefId')
                            pages = adp.get('dpPages')
                            annotation_type = adp.get('annotationType')
                            if annotation_type is None:
                                annotation_type = 'curated'
                            local_crossref_id = ""
                            prefix = cross_ref_id
                            if pages is not None and len(pages) > 0:
                                for page in pages:
                                    mod_global_cross_ref_id = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                              self.xref_url_map,
                                                                                              prefix,
                                                                                              page)
                                    xref = ETLHelper.get_xref_dict(local_crossref_id,
                                                                   prefix,
                                                                   page,
                                                                   page,
                                                                   cross_ref_id,
                                                                   mod_global_cross_ref_id,
                                                                   cross_ref_id + page + annotation_type)
                                    xref['dataId'] = disease_unique_key
                                    if 'loaded' in annotation_type:
                                        xref['loadedDB'] = 'true'
                                        xref['curatedDB'] = 'false'
                                    else:
                                        xref['curatedDB'] = 'true'
                                        xref['loadedDB'] = 'false'


                                    xrefs.append(xref)
                allele_list_to_yield.append(disease_record)
            else:
                disease_record = DiseaseHelper.get_disease_record(disease_record,
                                                                  data_providers,
                                                                  date_produced,
                                                                  data_provider)
                if disease_record is not None:

                    pecj_primary_key = disease_record.get('pecjPrimaryKey')

                    for ecode in disease_record.get('ecodes'):
                        ecode_map = {"pecjPrimaryKey": pecj_primary_key,
                                     "ecode": ecode}
                        evidence_code_list_to_yield.append(ecode_map)

                    disease_unique_key = disease_record.get('objectId') + \
                                         disease_record.get('DOid') + \
                                         disease_record['objectRelation'].get("associationType").upper()

                    if disease_record.get('pgeIds') is not None:
                        for pge in disease_record.get('pgeIds'):
                            pge_map = {"pecjPrimaryKey": pecj_primary_key,
                                       "pgeId": pge}
                            pge_list_to_yield.append(pge_map)

                    if 'with' in disease_record:
                        with_record = disease_record.get('with')
                        for rec in with_record:
                            with_map = {
                                "diseaseUniqueKey": disease_unique_key,
                                "withD": rec
                            }
                            withs.append(with_map)

                    if 'annotationDP' in disease_record:
                        for adp in disease_record['annotationDP']:
                            cross_ref_id = adp.get('crossRefId')
                            pages = adp.get('dpPages')
                            annotation_type = adp.get('annotationType')
                            local_crossref_id = ""
                            prefix = cross_ref_id
                            if annotation_type is None:
                                annotation_type = 'curated'
                            if pages is not None and len(pages) > 0:
                                for page in pages:
                                    mod_global_cross_ref_id = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                              self.xref_url_map,
                                                                                              prefix,
                                                                                              page)
                                    xref = ETLHelper.get_xref_dict(local_crossref_id,
                                                                   prefix,
                                                                   page,
                                                                   page,
                                                                   cross_ref_id,
                                                                   mod_global_cross_ref_id,
                                                                   cross_ref_id + page + annotation_type)
                                    xref['dataId'] = disease_unique_key
                                    if 'loaded' in annotation_type:
                                        xref['loadedDB'] = 'true'
                                        xref['curatedDB'] = 'false'
                                    else:
                                        xref['curatedDB'] = 'true'
                                        xref['loadedDB'] = 'false'

                                    xrefs.append(xref)
                agm_list_to_yield.append(disease_record)

            if counter == batch_size:
                yield [allele_list_to_yield,
                       gene_list_to_yield,
                       agm_list_to_yield,
                       pge_list_to_yield,
                       pge_list_to_yield,
                       pge_list_to_yield,
                       withs,
                       evidence_code_list_to_yield,
                       xrefs]

                agm_list_to_yield = []
                allele_list_to_yield = []
                gene_list_to_yield = []
                evidence_code_list_to_yield = []
                pge_list_to_yield = []
                xrefs = []
                withs = []
                counter = 0

        if counter > 0:
            yield [allele_list_to_yield,
                   gene_list_to_yield,
                   agm_list_to_yield,
                   pge_list_to_yield,
                   pge_list_to_yield,
                   pge_list_to_yield,
                   withs,
                   evidence_code_list_to_yield,
                   xrefs]

