'''GEO XREF ETL'''

import json
import time
import random
import logging
import urllib
import xmltodict

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from files import Download
from transactors import CSVTransactor, Neo4jTransactor



class GeoXrefETL(ETL):
    '''GEO XREF ETL'''

    logger = logging.getLogger(__name__)

    geo_xref_query = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
        MATCH (o:Gene) where o.primaryKey = row.genePrimaryKey
        """ + ETLHelper.get_cypher_xref_text()


    def __init__(self, config):
        super().__init__()
        self.data_type_config = config


    def _load_and_process_data(self):

        for sub_type in self.data_type_config.get_sub_type_objects():

            species_encoded = urllib.parse.quote_plus(\
                    ETLHelper.species_lookup_by_data_provider(sub_type.get_data_provider()))

            commit_size = self.data_type_config.get_neo4j_commit_size()
            #batch_size = self.data_type_config.get_generator_batch_size()
            batch_size = 100000

            generators = self.get_generators(batch_size, species_encoded)

            query_list = [
                [self.geo_xref_query, commit_size,
                 "geo_xref_data_" + sub_type.get_data_provider() + ".csv"],
            ]

            query_and_file_list = self.process_query_params(query_list)
            CSVTransactor.save_file_static(generators, query_and_file_list)
            Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, batch_size, species_encoded):
        '''Get Generators'''

        entrez_ids = []

        time.sleep(random.randint(0, 10)) # So that Geo wont fails on "TO MANY URL REQUESTS"
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi" \
                + "?term=gene_geoprofiles[filter]+AND+%s[Organism]&retmax=%s&db=gene" \
                % (species_encoded, batch_size)

        self.logger.info("Geo Url: %s", url)

        geo_data_file_contents = Download("tmp", url, "geo" + species_encoded).get_downloaded_data()
        geo_data = json.loads(json.dumps(xmltodict.parse(geo_data_file_contents)))

        for efetch_value in dict(geo_data.items()).values():
            # IdList is a value returned from efetch XML spec,
            # within IdList, there is another map with "Id"
            # as the key and the entrez local ids a list value.
            for sub_map_key, sub_map_value in efetch_value.items():
                if sub_map_key == 'IdList':
                    for id_list in dict(sub_map_value.items()).values():
                        for entrez_id in id_list:
                            # print ("here is the entrezid: " +entrezId)
                            entrez_ids.append("NCBI_Gene:" + entrez_id)

        query = """MATCH (g:Gene)-[crr:CROSS_REFERENCE]-(cr:CrossReference)
                   WHERE cr.globalCrossRefId IN {parameter}
                   RETURN g.primaryKey, g.modLocalId, cr.name, cr.globalCrossRefId"""

        geo_data_list = []
        return_set = Neo4jHelper.run_single_parameter_query(query, entrez_ids)

        for record in return_set:

            gene_primary_key = record["g.primaryKey"]
            mod_local_id = record["g.modLocalId"]
            global_cross_ref_id = record["cr.globalCrossRefId"]
            geo_xref = ETLHelper.get_xref_dict(global_cross_ref_id.split(":")[1],
                                               "NCBI_Gene",
                                               "gene/other_expression",
                                               "gene/other_expression",
                                               "GEO",
                                               "https://www.ncbi.nlm.nih.gov/sites/entrez?" \
                                                       + "Db=geoprofiles"\
                                                       + "&DbFrom=gene"\
                                                       + "&Cmd=Link"\
                                                       + "&LinkName=gene_geoprofiles"\
                                                       + "&LinkReadableName=GEO%20Profiles"\
                                                       + "&IdsFromResult="\
                                                       + global_cross_ref_id.split(":")[1],
                                               global_cross_ref_id+"gene/other_expression")

            geo_xref["genePrimaryKey"] = gene_primary_key
            geo_xref["modLocalId"] = mod_local_id

            geo_data_list.append(geo_xref)

        yield [geo_data_list]
