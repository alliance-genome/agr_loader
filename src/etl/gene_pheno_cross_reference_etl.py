"""Gene Pheno XREF ETL."""

import logging

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor


class GenePhenoCrossReferenceETL(ETL):
    """Gene Pheno XREF ETL."""

    logger = logging.getLogger(__name__)

    pheno_xref_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (o:Gene) where o.primaryKey = row.genePrimaryKey
        """ + ETLHelper.get_cypher_xref_text()

    gene_pheno_query_template = """
                   MATCH (g:Gene)-[gp:HAS_PHENOTYPE]-(p:Phenotype)
                   RETURN g.primaryKey, g.dataProvider"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        for sub_type in self.data_type_config.get_sub_type_objects():

            commit_size = self.data_type_config.get_neo4j_commit_size()

            generators = self.get_generators()

            query_template_list = [
                [self.pheno_xref_query_template, commit_size,
                 "pheno_xref_data_" + sub_type.get_data_provider() + ".csv"],
            ]

            query_and_file_list = self.process_query_params(query_template_list)
            CSVTransactor.save_file_static(generators, query_and_file_list)
            Neo4jTransactor.execute_query_batch(query_and_file_list)
            self.error_messages()

    def get_generators(self):
        """Get Generators."""

        gene_pheno_data_list = []
        return_set = Neo4jHelper.run_single_parameter_query(self.gene_pheno_query_template)

        for record in return_set:
            global_cross_ref_id = record["g.primaryKey"]
            data_provider = record["g.dataProvider"]
            id_prefix = global_cross_ref_id.split(":")[0]
            if data_provider != 'MGI':
                page = 'gene/phenotypes_impc'
                url = self.etlh.rdh2.return_url_from_key_value(id_prefix, global_cross_ref_id.split(":")[1], page)
                self.logger.info(url)
                gene_pheno_xref = ETLHelper.get_xref_dict(global_cross_ref_id.split(":")[1],
                                                          id_prefix,
                                                          page,
                                                          page,
                                                          id_prefix,
                                                          url,
                                                          global_cross_ref_id+page)
            else:
                page = 'gene/phenotypes'
                url = self.etlh.rdh2.return_url_from_key_value(id_prefix, global_cross_ref_id.split(":")[1], page)
                self.logger.info(url)
                gene_pheno_xref = ETLHelper.get_xref_dict(global_cross_ref_id.split(":")[1],
                                                          id_prefix,
                                                          page,
                                                          page,
                                                          id_prefix,
                                                          url,
                                                          global_cross_ref_id+page)

            gene_pheno_xref["genePrimaryKey"] = global_cross_ref_id

            gene_pheno_data_list.append(gene_pheno_xref)

        yield [gene_pheno_data_list]
