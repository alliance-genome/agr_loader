"""Phenotype ETL."""

import logging
import uuid
import multiprocessing

from etl import ETL
from etl.helpers import ExperimentalConditionHelper
from files import JSONFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class PhenoTypeETL(ETL):
    """Phenotype ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    execute_allele_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (allele:Allele {primaryKey:row.primaryId})


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
            MERGE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
                ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join'

                MERGE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)

                MERGE (pa)-[pubfpubEE:EVIDENCE]->(pubEJ)

            }
        IN TRANSACTIONS of %s ROWS"""
    
    execute_gene_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

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
            MERGE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
                ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join'

                MERGE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)

                MERGE (pa)-[pubfpubEE:EVIDENCE]->(pubEJ)

            }
        IN TRANSACTIONS of %s ROWS"""

    execute_agm_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

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
                MERGE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
                ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join'

                MERGE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)

                MERGE (pa)-[pubfpubEE:EVIDENCE]->(pubEJ)

            }
        IN TRANSACTIONS of %s ROWS"""

    execute_pges_allele_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row
                MATCH (n:Allele {primaryKey:row.pgeId})
                MATCH (d:PublicationJoin {primaryKey:row.pecjPrimaryKey})

                CREATE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]->(n)

            }
        IN TRANSACTIONS of %s ROWS"""

    execute_pges_agm_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row
                MATCH (n:AffectedGenomicModel {primaryKey:row.pgeId})
                MATCH (d:PublicationJoin {primaryKey:row.pecjPrimaryKey})

                CREATE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]->(n)

            }
        IN TRANSACTIONS of %s ROWS"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

        self.exp_cond_helper = ExperimentalConditionHelper("PhenotypeEntityJoin")

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
        data_provider = sub_type.get_data_provider()
        self.logger.info("subtype: " + data_provider)

        query_template_list = [
                [self.execute_gene_query_template,
                 "phenotype_gene_data_" + sub_type.get_data_provider() + ".csv", commit_size],
                [self.execute_allele_query_template,
                 "phenotype_allele_data_" + sub_type.get_data_provider() + ".csv", commit_size],
                [self.execute_agm_query_template,
                 "phenotype_agm_data_" + sub_type.get_data_provider() + ".csv", commit_size],
                [self.exp_cond_helper.execute_exp_condition_query_template,
                 "phenotype_exp_condition_node_data_" + sub_type.get_data_provider() + ".csv", commit_size],
                [self.exp_cond_helper.execute_exp_condition_relations_query_template,
                 "phenotype_exp_condition_rel_data_" + sub_type.get_data_provider() + ".csv", commit_size],
                [self.execute_pges_allele_query_template,
                 "phenotype_pges_allele_data_" + sub_type.get_data_provider() + ".csv", commit_size],
                [self.execute_pges_agm_query_template,
                 "phenotype_pges_agm_data_" + sub_type.get_data_provider() + ".csv", commit_size]
        ]

        # Obtain the generator
        generators = self.get_generators(data, batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("Phenotype-{}: ".format(sub_type.get_data_provider()))

    def pub_process(self, evidence, primary_id):
        """Process pubs.

        return dict of pubs.
        """
        pub_med_url = None
        pub_mod_url = None
        if 'publicationId' not in evidence:
            self.logger.info("%s has no publicationId", primary_id)
            pub_med_id = ""
            pub_mod_id = ""
        else:
            pub_med_id = None
            pub_mod_id = None
            if evidence.get('publicationId').startswith('PMID:'):
                pub_med_id = evidence['publicationId']
                local_pub_med_id = pub_med_id.split(":")[1]
                pub_med_prefix = pub_med_id.split(":")[0]
                pub_med_url = self.etlh.get_no_page_complete_url(
                    local_pub_med_id, pub_med_prefix, primary_id)
                if 'crossReference' in evidence:
                    pub_xref = evidence.get('crossReference')
                    pub_mod_id = pub_xref.get('id')
                    if pub_mod_id is not None:
                        pub_mod_url = self.etlh.rdh2.return_url_from_identifier(pub_mod_id)
            else:
                pub_mod_id = evidence.get('publicationId')
                if pub_mod_id is not None:
                    pub_mod_url = self.etlh.rdh2.return_url_from_identifier(pub_mod_id)

        if pub_mod_id is None and pub_med_id is None:
            self.logger.info("%s is missing pubMed and pubMod id", primary_id)

        if pub_med_id is None:
            pub_med_id = ""

        if pub_mod_id is None:
            pub_mod_id = ""

        return {'pub_med_url': pub_med_url,
                'pub_med_id': pub_med_id,
                'pub_mod_url': pub_mod_url,
                'pub_mod_id': pub_mod_id
                }

    def primary_genetic_entity_process(self, pheno, pge_list_to_yield, pecj_primary_key):
        """Process Primary Genetic EntityIDs."""
        if 'primaryGeneticEntityIDs' not in pheno:
            return
        pge_ids = pheno.get('primaryGeneticEntityIDs')
        for pge in pge_ids:
            # pge_key = pge_key + pge
            pge_map = {"pecjPrimaryKey": pecj_primary_key,
                       "pgeId": pge}
            pge_list_to_yield.append(pge_map)

    def get_generators(self, phenotype_data, batch_size):
        """Get Generators."""
        list_to_yield = []
        self.exp_cond_helper.reset()
        pge_list_to_yield = []
        calculated_cross_references = []
        date_produced = phenotype_data['metaData']['dateProduced']
        data_providers = []
        counter = 0
        # pge_key = ''

        self.data_providers_process(phenotype_data)
        load_key = date_produced + self.data_provider + "_phenotype"

        for pheno in phenotype_data['data']:
            pecj_primary_key = str(uuid.uuid4())
            counter = counter + 1
            primary_id = pheno.get('objectId')
            phenotype_statement = pheno.get('phenotypeStatement')

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(primary_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            evidence = pheno.get('evidence')

            pubs = self.pub_process(evidence, primary_id)

            date_assigned = pheno.get('dateAssigned')

            ec_unique_key_concat = self.exp_cond_helper.conditionrelations_process(pheno)

            # phenotype_unique_key formatted to represent (readably):
            #  object `a` under conditions `b` is associated with phenotype `c`

            #object `a`
            phenotype_unique_key = primary_id

            #conditions `b`
            # Combination of unique condition keys must be included in the phenotype_unique_key
            # in order to create unique PhenotypeEntityJoin nodes per condition combo.
            phenotype_unique_key += ec_unique_key_concat

            #phenotype `c`
            phenotype_unique_key += phenotype_statement.strip()

            #Add this disease_unique_key to every experimental condition relation
            # and commit the result
            self.exp_cond_helper.complete_and_commit_record_cond_rels(phenotype_unique_key)

            self.primary_genetic_entity_process(pheno, pge_list_to_yield, pecj_primary_key)
            phenotype = {
                "primaryId": primary_id,
                "phenotypeUniqueKey": phenotype_unique_key,
                "phenotypeStatement": phenotype_statement.strip(),
                "dateAssigned": date_assigned,
                "loadKey": load_key,
                "type": "gene",
                "dataProviders": data_providers,
                "dataProvider": self.data_provider,
                "dateProduced": date_produced,
                "pubMedId": pubs['pub_med_id'],
                "pubMedUrl": pubs['pub_med_url'],
                "pubModId": pubs['pub_mod_id'],
                "pubModUrl": pubs['pub_mod_url'],
                "pubPrimaryKey": pubs['pub_med_id'] + pubs['pub_mod_id'],
                "pecjPrimaryKey": pecj_primary_key
            }

            list_to_yield.append(phenotype)

            if counter == batch_size:
                yield [ list_to_yield,
                        list_to_yield,
                        list_to_yield,
                        self.exp_cond_helper.get_cond_nodes(),
                        self.exp_cond_helper.get_cond_rels(),
                        pge_list_to_yield,
                        pge_list_to_yield ]

                list_to_yield = []
                self.exp_cond_helper.reset()
                pge_list_to_yield = []
                counter = 0

        if counter > 0:
            yield [ list_to_yield,
                    list_to_yield,
                    list_to_yield,
                    self.exp_cond_helper.get_cond_nodes(),
                    self.exp_cond_helper.get_cond_rels(),
                    pge_list_to_yield,
                    pge_list_to_yield ]
