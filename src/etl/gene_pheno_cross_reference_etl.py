"""Gene Pheno XREF ETL."""

import logging

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor


class GenePhenoCrossReferenceETL(ETL):
    """Gene Pheno XREF ETL."""

    logger = logging.getLogger(__name__)

    pheno_xref_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (o:Gene {primaryKey:row.genePrimaryKey})
                """ + ETLHelper.get_cypher_xref_tuned_text() + """
            }
        IN TRANSACTIONS of %s ROWS"""

    pheno_xref_relations_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (o:Gene {primaryKey:row.genePrimaryKey})
                MATCH (id:CrossReference {primaryKey:row.primaryKey})

                MERGE (o)-[gcr:CROSS_REFERENCE]->(id)
            }
        IN TRANSACTIONS of %s ROWS"""

    gene_pheno_query_template = """
                   MATCH (g:Gene)-[gp:HAS_PHENOTYPE]-(p:Phenotype)
                   RETURN distinct g.primaryKey, g.dataProvider"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        """Load and process data."""
        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()
        generators = self.get_generators(batch_size)

        query_template_list = [
                [self.pheno_xref_query_template,
                 "pheno_xref_data_" + ".csv", commit_size],
                [self.pheno_xref_relations_template,
                 "pheno_xref_relations_data_" + ".csv", commit_size],
        ]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages()

    def get_generators(self, batch_size):
        """Get Generators."""
        gene_pheno_data_list = []
        with Neo4jHelper.run_single_query(self.gene_pheno_query_template) as return_set:
            counter = 0

            for record in return_set:
                counter = counter + 1
                global_cross_ref_id = record["g.primaryKey"]
                data_provider = record["g.dataProvider"]
                id_prefix = global_cross_ref_id.split(":")[0]
                if data_provider == 'MGI':
                    page = 'gene/phenotypes_impc'
                    url = self.etlh.rdh2.return_url_from_key_value(id_prefix, global_cross_ref_id.split(":")[1], page)
                    gene_pheno_xref = ETLHelper.get_xref_dict(global_cross_ref_id.split(":")[1],
                                                              id_prefix,
                                                              page,
                                                              page,
                                                              "IMPC",
                                                              url,
                                                              global_cross_ref_id+page)
                elif data_provider == 'HUMAN' or id_prefix == 'HGNC':
                    continue
                else:
                    page = 'gene/phenotypes'
                    url = self.etlh.rdh2.return_url_from_key_value(id_prefix, global_cross_ref_id.split(":")[1], page)
                    gene_pheno_xref = ETLHelper.get_xref_dict(global_cross_ref_id.split(":")[1],
                                                              id_prefix,
                                                              page,
                                                              page,
                                                              id_prefix,
                                                              url,
                                                              global_cross_ref_id+page)

                gene_pheno_xref["genePrimaryKey"] = global_cross_ref_id

                gene_pheno_data_list.append(gene_pheno_xref)

                if counter == batch_size:
                    yield [gene_pheno_data_list, gene_pheno_data_list]
                    gene_pheno_data_list = []

            if counter > 0:
                yield [gene_pheno_data_list, gene_pheno_data_list]
