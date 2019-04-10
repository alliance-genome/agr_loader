import logging, sys, uuid
logger = logging.getLogger(__name__)

import multiprocessing
import xmltodict

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)

class ExpressionAtlasETL(ETL):
    get_all_gene_primary_to_ensmbl_ids_query = """
        MATCH (g:Gene)-[:CROSS_REFERENCE]-(c:CrossReference)
        WHERE c.globalCrossRefId STARTS WITH 'ENSEMBL:'
        RETURN g.primaryKey, c.globalCrossRefId"""

    get_genes_with_expression_atlas_links_query = """
        MATCH (g:Gene)
        WHERE LOWER(g.primaryKey) IN {parameter}
        RETURN g.primaryKey, g.modLocalId"""

    add_expression_atlas_crossreferences_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (o:Gene)
        WHERE o.primaryKey = row.genePrimaryKey """  + ETLHelper.get_cypher_xref_text()

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []
        ensgToGenePrimaryIdMap = self._get_primary_gene_ids_to_ensembl_ids()
        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type, ensgToGenePrimaryIdMap))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

    def _get_primary_gene_ids_to_ensembl_ids(self):
        returnSet = Neo4jHelper.run_single_query(ExpressionAtlasETL.get_all_gene_primary_to_ensmbl_ids_query)

        primaryIdToEnsgId = dict()
        for record in returnSet:
            primaryIdToEnsgId[record["c.globalCrossRefId"].split(":")[1].lower()] = record["g.primaryKey"]

        return primaryIdToEnsgId

    # Returns only pages for genes that we have in the Alliance
    def _get_expression_atlas_gene_pages(self, sub_type, dataProvider, ensgToGenePrimaryIdMap):
        filepath = sub_type.get_filepath()

        expressionAtlasGenePages = {}
        with open(filepath) as fd:
            doc = xmltodict.parse(fd.read())["urlset"]
            for value in doc.values():
                if isinstance(value, (list,)):
                    for element in value:
                        url = element['loc']
                        expressionAtlasGene = url.split("/")[-1]
                        if expressionAtlasGene in ensgToGenePrimaryIdMap:
                            expressionAtlasGenePages[ensgToGenePrimaryIdMap[expressionAtlasGene].lower()] = url
                        else:
                            allianceGene = dataProvider + ":" + expressionAtlasGene
                            expressionAtlasGenePages[allianceGene.lower()] = url

        return expressionAtlasGenePages

    def _process_sub_type(self, sub_type, ensgToGenePrimaryIdMap):
        dataProvider = sub_type.get_data_provider()
        expressionAtlasGenePages = self._get_expression_atlas_gene_pages(sub_type, dataProvider, ensgToGenePrimaryIdMap)

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        generators = self.get_generators(expressionAtlasGenePages, dataProvider, batch_size)

        query_list = [
            [ExpressionAtlasETL.add_expression_atlas_crossreferences_template, commit_size, "expression_atlas_" + dataProvider  + "_data.csv"],
        ]

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, expressionAtlasGenePages, dataProvider, batch_size):
        returnSet = Neo4jHelper.run_single_parameter_query(ExpressionAtlasETL.get_genes_with_expression_atlas_links_query,
                                                           list(expressionAtlasGenePages.keys()))

        counter = 0
        cross_reference_list = []
        for record in returnSet:
            counter += 1
            cross_reference = ETLHelper.get_xref_dict(record["g.primaryKey"].split(":")[1],
                                                      "ExpressionAtlas_gene",
                                                      "gene/expression-atlas",
                                                      "gene/expressionAtlas",
                                                      record["g.modLocalId"],
                                                      expressionAtlasGenePages[record["g.primaryKey"].lower()],
                                                      dataProvider + ":" + record["g.modLocalId"] + "gene/expression-atlas")
            cross_reference["genePrimaryKey"] = record["g.primaryKey"]
            cross_reference_list.append(cross_reference)
            if counter > batch_size:
                yield [cross_reference_list]
                counter =0
                cross_reference_list = []

        if counter > 0:
            yield [cross_reference_list]
