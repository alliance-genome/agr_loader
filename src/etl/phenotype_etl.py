''''Phenotype ETL'''

import logging
import uuid
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class PhenoTypeETL(ETL):
    '''Phenotype ETL'''

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    execute_allele_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (allele:Allele:Feature {primaryKey:row.primaryId})
            

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                ON CREATE SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (pa:PhenotypeEntityJoin:Association {primaryKey:row.phenotypeUniqueKey})
                ON CREATE SET 
                    pa.joinType = 'phenotype',
                    pa.dataProvider = row.dataProvider

            MERGE (allele)-[:HAS_PHENOTYPE {uuid:row.phenotypeUniqueKey}]->(p)

            MERGE (allele)-[fpaf:ASSOCIATION]->(pa)
            MERGE (pa)-[pad:ASSOCIATION]->(p)
            
             MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl
           
                       //MERGE (pubf)-[pe:EVIDENCE]-(pa)
           CREATE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
             SET pubEJ.joinType = 'pub_evidence_code_join'

            CREATE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)
            
            CREATE (pa)-[pubfpubEE:EVIDENCE]->(pubEJ)
            
            """

    execute_allele_gene_pej_relationship_query_template = """
    
    USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            // GET PRIMARY DATA OBJECTS
        MATCH (pej:PublicationJoin {primaryKey:row.pecjPrimaryKey})
        MATCH (allele:Allele:Feature {primaryKey:row.primaryId})
        MATCH (g:Gene)-[a:IS_ALLELE_OF]-(allele)

        MERGE (g)-[gapf:ASSOCIATION]->(pej)

    """
    execute_gene_query_template = """

            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:Gene {primaryKey:row.primaryId})

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                ON CREATE SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (pa:PhenotypeEntityJoin:Association {primaryKey:row.phenotypeUniqueKey})
                ON CREATE SET 
                    pa.joinType = 'phenotype',
                    pa.dataProvider = row.dataProvider
            
                MERGE (pa)-[pad:ASSOCIATION]->(p)
                MERGE (g)-[gpa:ASSOCIATION]->(pa)
                MERGE (g)-[genep:HAS_PHENOTYPE {uuid:row.phenotypeUniqueKey}]->(p)
                
             MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl
           
                       //MERGE (pubf)-[pe:EVIDENCE]-(pa)
           CREATE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
             SET pubEJ.joinType = 'pub_evidence_code_join'

            CREATE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)
            
            CREATE (pa)-[pubfpubEE:EVIDENCE]->(pubEJ)

            """

    execute_agm_query_template = """

            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:AffectedGenomicModel {primaryKey:row.primaryId})

            MERGE (p:Phenotype {primaryKey:row.phenotypeStatement})
                ON CREATE SET p.phenotypeStatement = row.phenotypeStatement

            MERGE (pa:PhenotypeEntityJoin:Association {primaryKey:row.phenotypeUniqueKey})
                ON CREATE SET 
                    pa.joinType = 'phenotype',
                    pa.dataProvider = row.dataProvider

                MERGE (pa)-[pad:ASSOCIATION]->(p)
                MERGE (g)-[gpa:ASSOCIATION]->(pa)
                MERGE (g)-[genep:HAS_PHENOTYPE {uuid:row.phenotypeUniqueKey}]->(p)
            
            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl
           
                       //MERGE (pubf)-[pe:EVIDENCE]-(pa)
            CREATE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
              SET pubEJ.joinType = 'pub_evidence_code_join'

            CREATE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)
            
            CREATE (pa)-[pubfpubEE:EVIDENCE]->(pubEJ)

    """

    execute_pges_allele_query_template = """

        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (n:Allele:Feature {primaryKey:row.pgeId})
            MATCH (d:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})

            CREATE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]->(n)

    """

    execute_pges_agm_query_template = """

        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (n:AffectedGenomicModel {primaryKey:row.pgeId})
            MATCH (d:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})

            CREATE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]->(n)

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

        self.logger.info("Loading Phenotype Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        self.logger.info("Finished Loading Phenotype Data: %s", sub_type.get_data_provider())

        if data is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
            return

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_neo4j_commit_size()
        query_template_allele_list = []
        query_template_agm_list = []
        data_provider = sub_type.get_data_provider()

        generators = self.get_generators(data, batch_size, data_provider)

        if data_provider in ['MGI', 'ZFIN', 'RGD']:
            query_template_list = [
                [self.execute_gene_query_template, commit_size,
                 "phenotype_gene_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_allele_query_template, commit_size,
                 "phenotype_allele_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_agm_query_template, commit_size,
                 "phenotype_agm_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_allele_gene_pej_relationship_query_template, commit_size,
                 "phenotype_allele_gene_pej_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_pges_agm_query_template, commit_size,
                 "phenotype_agm_pge_data_" + sub_type.get_data_provider() + ".csv"]
            ]
            query_template_agm_list = [
                [self.execute_pges_agm_query_template, commit_size,
                 "phenotype_agm_pge_data_" + sub_type.get_data_provider() + ".csv"]
            ]
            basic_query_template_list = [
                [self.execute_gene_query_template, commit_size,
                 "phenotype_gene_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_allele_query_template, commit_size,
                 "phenotype_allele_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_allele_gene_pej_relationship_query_template, commit_size,
                 "phenotype_allele_gene_pej_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_agm_query_template, commit_size,
                 "phenotype_agm_data_" + sub_type.get_data_provider() + ".csv"]
            ]

        elif data_provider in ['FB', 'WB']:
            query_template_list = [
                [self.execute_gene_query_template, commit_size,
                 "phenotype_gene_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_allele_query_template, commit_size,
                 "phenotype_allele_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_allele_gene_pej_relationship_query_template, commit_size,
                 "phenotype_allele_gene_pej_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_pges_allele_query_template, commit_size,
                 "phenotype_allele_pge_data_" + sub_type.get_data_provider() + ".csv"]
            ]
            query_template_allele_list = [
                [self.execute_pges_allele_query_template, commit_size,
                 "phenotype_allele_pge_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_allele_gene_pej_relationship_query_template, commit_size,
                 "phenotype_allele_gene_pej_data_" + sub_type.get_data_provider() + ".csv"]
            ]
            basic_query_template_list = [
                [self.execute_gene_query_template, commit_size,
                 "phenotype_gene_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_allele_query_template, commit_size,
                 "phenotype_allele_data_" + sub_type.get_data_provider() + ".csv"],
                [self.execute_allele_gene_pej_relationship_query_template, commit_size,
                 "phenotype_allele_gene_pej_data_" + sub_type.get_data_provider() + ".csv"]
            ]
        else:
            query_template_list = [
                [self.execute_gene_query_template, commit_size,
                 "phenotype_gene_data_" + sub_type.get_data_provider() + ".csv"]
            ]
            basic_query_template_list = [
                [self.execute_gene_query_template, commit_size,
                 "phenotype_gene_data_" + sub_type.get_data_provider() + ".csv"]
            ]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)

        basic_query_and_file_list = self.process_query_params(basic_query_template_list)
        Neo4jTransactor.execute_query_batch(basic_query_and_file_list)

        Neo4jTransactor.wait_for_queues()

        query_agm_and_file_list = self.process_query_params(query_template_agm_list)
        Neo4jTransactor.execute_query_batch(query_agm_and_file_list)

        query_allele_and_file_list = self.process_query_params(query_template_allele_list)
        Neo4jTransactor.execute_query_batch(query_allele_and_file_list)


    def get_generators(self, phenotype_data, batch_size, data_provider):
        '''Get Generators'''

        list_to_yield = []
        pge_list_to_yield = []
        date_produced = phenotype_data['metaData']['dateProduced']
        data_providers = []
        data_provider_object = phenotype_data['metaData']['dataProvider']
        counter = 0
        data_provider_cross_ref = data_provider_object.get('crossReference')
        data_provider = data_provider_cross_ref.get('id')
        data_provider_pages = data_provider_cross_ref.get('pages')
        data_provider_cross_ref_set = []
        pge_key = ''

        load_key = date_produced + data_provider + "_phenotype"

        if data_provider_pages is not None:
            for data_provider_page in data_provider_pages:
                cross_ref_complete_url = ETLHelper.get_page_complete_url(data_provider,
                                                                         ETL.xref_url_map,
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
                self.logger.debug("data provider: %s", data_provider)

        for pheno in phenotype_data['data']:
            pecj_primary_key = str(uuid.uuid4())
            counter = counter + 1
            pub_med_id = None
            pub_mod_id = None
            pub_med_url = None
            pub_mod_url = None
            primary_id = pheno.get('objectId')
            phenotype_statement = pheno.get('phenotypeStatement')

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(primary_id)
                if is_it_test_entry is False:
                    continue

            evidence = pheno.get('evidence')

            if 'publicationId' in evidence:
                if evidence.get('publicationId').startswith('PMID:'):
                    pub_med_id = evidence['publicationId']
                    local_pub_med_id = pub_med_id.split(":")[1]
                    pub_med_prefix = pub_med_id.split(":")[0]
                    pub_med_url = ETLHelper.get_no_page_complete_url(local_pub_med_id,
                                                                     self.xref_url_map,
                                                                     pub_med_prefix,
                                                                     primary_id)
                    if pub_med_id is None:
                        pub_med_id = ""

                    if 'crossReference' in evidence:
                        pub_xref = evidence.get('crossReference')
                        pub_mod_id = pub_xref.get('id')
                        pub_mod_local_id = pub_mod_id.split(":")[1]
                        if pub_mod_id is not None:
                            pub_mod_url = ETLHelper.get_complete_pub_url(pub_mod_local_id,
                                                                         pub_mod_id)

                else:
                    pub_mod_id = evidence.get('publicationId')
                    if pub_mod_id is not None:
                        pub_mod_local_id = pub_mod_id.split(":")[1]
                        pub_mod_url = ETLHelper.get_complete_pub_url(pub_mod_local_id, pub_mod_id)

                if pub_mod_id is None:
                    pub_mod_id = ""

            if pub_med_id is None:
                pub_med_id = ""

            if pub_mod_id is None:
                pub_mod_id = ""

            date_assigned = pheno.get('dateAssigned')

            if pub_mod_id is None and pub_med_id is None:
                self.logger.info("%s is missing pubMed and pubMod id", primary_id)

            if 'primaryGeneticEntityIDs' in pheno:
                pge_ids = pheno.get('primaryGeneticEntityIDs')
                for pge in pge_ids:
                    pge_key = pge_key + pge
                    pge_map = {"pecjPrimaryKey": pecj_primary_key,
                               "pgeId": pge}
                    pge_list_to_yield.append(pge_map)

            else:
                pge_ids = []

            phenotype = {
                "primaryId": primary_id.strip(),
                "phenotypeUniqueKey": primary_id.strip() + phenotype_statement.strip(),
                "phenotypeStatement": phenotype_statement.strip(),
                "dateAssigned": date_assigned,
                "loadKey": load_key,
                "type": "gene",
                "dataProviders": data_providers,
                "dataProvider": data_provider,
                "dateProduced": date_produced,
                "pubMedId": pub_med_id,
                "pubMedUrl": pub_med_url,
                "pubModId": pub_mod_id,
                "pubModUrl": pub_mod_url,
                "pubPrimaryKey": pub_med_id + pub_mod_id,
                "pecjPrimaryKey": pecj_primary_key
            }

            list_to_yield.append(phenotype)

            if counter == batch_size:
                if data_provider == 'SGD':
                    yield [list_to_yield]
                    list_to_yield = []
                    counter = 0
                elif data_provider in ['FB', 'WB']:
                    yield [list_to_yield, list_to_yield, list_to_yield, pge_list_to_yield]
                    list_to_yield = []
                    pge_list_to_yield = []
                    counter = 0
                else:
                    yield [list_to_yield,
                           list_to_yield,
                           list_to_yield,
                           list_to_yield,
                           pge_list_to_yield]
                    list_to_yield = []
                    pge_list_to_yield = []
                    counter = 0

        if counter > 0:
            if data_provider == 'SGD':
                yield [list_to_yield]
            elif data_provider in ['FB', 'WB']:
                yield [list_to_yield, list_to_yield, list_to_yield, pge_list_to_yield]
            else:
                yield [list_to_yield,
                       list_to_yield,
                       list_to_yield,
                       list_to_yield,
                       pge_list_to_yield]
