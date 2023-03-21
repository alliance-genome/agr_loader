"""GEO XREF ETL."""

import json
import logging
import urllib
import xmltodict
from pathlib import Path

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor


class GeoXrefETL(ETL):
    """GEO XREF ETL."""

    logger = logging.getLogger(__name__)

    geo_xref_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

            MATCH (o:Gene) where o.primaryKey = row.genePrimaryKey
            """ + ETLHelper.get_cypher_xref_text() + """
            }
        IN TRANSACTIONS of %s ROWS"""

    gene_crossref_query_template = """
                   MATCH (g:Gene)-[crr:CROSS_REFERENCE]-(cr:CrossReference)
                   WHERE cr.globalCrossRefId IN {parameter}
                   RETURN g.primaryKey, g.modLocalId, cr.name, cr.globalCrossRefId"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        for sub_type in self.data_type_config.get_sub_type_objects():

            commit_size = self.data_type_config.get_neo4j_commit_size()

            generators = self.get_generators(sub_type)

            query_template_list = [
                [self.geo_xref_query_template, 
                 "geo_xref_data_" + sub_type.get_data_provider() + ".csv", commit_size],
            ]

            query_and_file_list = self.process_query_params(query_template_list)
            CSVTransactor.save_file_static(generators, query_and_file_list)
            Neo4jTransactor.execute_query_batch(query_and_file_list)
            self.error_messages()

    def get_generators(self, sub_type):
        """Get Generators."""
        entrez_ids = []

        geo_data_file_contents = Path(sub_type.get_filepath()).read_text()
        geo_data = json.loads(json.dumps(xmltodict.parse(geo_data_file_contents)))
        for efetch_value in dict(geo_data.items()).values():
            # IdList is a value returned from efetch XML spec,
            # within IdList, there is another map with "Id"
            # as the key and the entrez local ids a list value.
            for sub_map_key, sub_map_value in efetch_value.items():
                if sub_map_key == 'IdList':
                    for id_list in dict(sub_map_value.items()).values():
                        for entrez_id in id_list:
                            self.logger.debug("here is the entrez id: %s", entrez_id)
                            entrez_ids.append("NCBI_Gene:" + entrez_id)

        geo_data_list = []
        with Neo4jHelper.run_single_parameter_query(self.gene_crossref_query_template, entrez_ids) as return_set:
            for record in return_set:
                gene_primary_key = record["g.primaryKey"]
                mod_local_id = record["g.modLocalId"]
                global_cross_ref_id = record["cr.globalCrossRefId"]
                url = self.etlh.rdh2.return_url_from_key_value('GEO', global_cross_ref_id.split(":")[1], 'entrezgene')
                geo_xref = ETLHelper.get_xref_dict(global_cross_ref_id.split(":")[1],
                                                   "NCBI_Gene",
                                                   "gene/other_expression",
                                                   "gene/other_expression",
                                                   "GEO",
                                                   url,
                                                   global_cross_ref_id+"gene/other_expression")

                geo_xref["genePrimaryKey"] = gene_primary_key
                geo_xref["modLocalId"] = mod_local_id

                geo_data_list.append(geo_xref)

        yield [geo_data_list]
