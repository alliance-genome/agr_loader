'''Expression Atlas ETL'''

import logging
import multiprocessing
import xmltodict

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor


class ExpressionAtlasETL(ETL):
    '''Expression Atlas ETL'''

    logger = logging.getLogger(__name__)

    # Querys which do not take params and can be used as is

    get_all_gene_primary_to_ensmbl_ids_query = """
        MATCH (g:Gene)-[:CROSS_REFERENCE]-(c:CrossReference)
        WHERE c.prefix = 'ENSEMBL'
        RETURN g.primaryKey, c.localId"""

    get_mod_gene_symbol_to_primary_ids_query = """
        MATCH (g:Gene)
        WHERE g.dataProvider = {parameter}
        RETURN g.primaryKey, g.symbol"""

    get_genes_with_expression_atlas_links_query = """
        MATCH (g:Gene)
        WHERE LOWER(g.primaryKey) IN {parameter}
        RETURN g.primaryKey, g.modLocalId"""

    # Query templates which take params and will be processed later

    add_expression_atlas_crossreferences_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (o:Gene)
        WHERE o.primaryKey = row.genePrimaryKey
        """  + ETLHelper.get_cypher_xref_text()

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []
        ensg_to_gene_primary_id_map = self._get_primary_gene_ids_to_ensembl_ids()

        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type,
                                              args=(sub_type, ensg_to_gene_primary_id_map))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

    @staticmethod
    def _get_primary_gene_ids_to_ensembl_ids():
        return_set = Neo4jHelper.run_single_query(\
                         ExpressionAtlasETL.get_all_gene_primary_to_ensmbl_ids_query)
        return {record["c.localId"].lower(): record["g.primaryKey"] for record in return_set}

    @staticmethod
    def _get_mod_gene_symbol_to_primary_ids(data_provider):
        return_set = Neo4jHelper.run_single_parameter_query(\
                         ExpressionAtlasETL.get_mod_gene_symbol_to_primary_ids_query,
                         data_provider)
        return {record["g.symbol"].lower(): record["g.primaryKey"] for record in return_set}


    # Returns only pages for genes that we have in the Alliance
    def _get_expression_atlas_gene_pages(self, sub_type,
                                         data_provider, ensg_to_gene_primary_id_map):
        filepath = sub_type.get_filepath()
        gene_symbol_to_primary_id_map = self._get_mod_gene_symbol_to_primary_ids(data_provider)

        expression_atlas_gene_pages = {}
        with open(filepath) as file_handle:
            doc = xmltodict.parse(file_handle.read())["urlset"]
            for value in doc.values():
                if isinstance(value, (list,)):
                    for element in value:
                        url = element['loc']
                        expression_atlas_gene = url.split("/")[-1]
                        expression_atlas_gene = expression_atlas_gene.lower()
                        if expression_atlas_gene in ensg_to_gene_primary_id_map:
                            expression_atlas_gene_pages[\
                                      ensg_to_gene_primary_id_map[expression_atlas_gene].lower()
                                      ] = url
                        elif expression_atlas_gene in gene_symbol_to_primary_id_map:
                            expression_atlas_gene_pages[\
                                      gene_symbol_to_primary_id_map[expression_atlas_gene].lower()
                                      ] = url
                        else:
                            alliance_gene = data_provider + ":" + expression_atlas_gene
                            expression_atlas_gene_pages[alliance_gene.lower()] = url

        return expression_atlas_gene_pages

    def _process_sub_type(self, sub_type, ensg_to_gene_primary_id_map):

        data_provider = sub_type.get_data_provider()
        expression_atlas_gene_pages = self._get_expression_atlas_gene_pages(\
                sub_type, data_provider, ensg_to_gene_primary_id_map)

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        generators = self.get_generators(expression_atlas_gene_pages, data_provider, batch_size)

        query_template_list = [
            [self.add_expression_atlas_crossreferences_query_template, commit_size,
             "expression_atlas_" + data_provider  + "_data.csv"],
        ]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, expression_atlas_gene_pages, data_provider, batch_size):
        '''Get Generators'''

        return_set = Neo4jHelper.run_single_parameter_query(\
                ExpressionAtlasETL.get_genes_with_expression_atlas_links_query,
                list(expression_atlas_gene_pages.keys()))

        counter = 0
        cross_reference_list = []
        for record in return_set:
            counter += 1
            cross_reference = ETLHelper.get_xref_dict(\
                    record["g.primaryKey"].split(":")[1],
                    "ExpressionAtlas_gene",
                    "gene/expression-atlas",
                    "gene/expressionAtlas",
                    record["g.modLocalId"],
                    expression_atlas_gene_pages[record["g.primaryKey"].lower()],
                    data_provider + ":" + record["g.modLocalId"] + "gene/expression-atlas")
            cross_reference["genePrimaryKey"] = record["g.primaryKey"]
            cross_reference_list.append(cross_reference)
            if counter > batch_size:
                yield [cross_reference_list]
                counter = 0
                cross_reference_list = []

        if counter > 0:
            yield [cross_reference_list]
