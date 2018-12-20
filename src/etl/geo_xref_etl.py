import json, time, random, multiprocessing
import logging, urllib, xmltodict

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from files import Download
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)

class GeoXrefETL(ETL):

    geoXrefQuery = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
        MATCH (o:Gene) where o.primaryKey = row.genePrimaryKey """ + ETLHelper.get_cypher_xref_text()

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        
        for sub_type in self.data_type_config.get_sub_type_objects():

            species_encoded = urllib.parse.quote_plus(ETLHelper.species_lookup_by_data_provider(sub_type.get_data_provider()))
    
            commit_size = self.data_type_config.get_neo4j_commit_size()
            #batch_size = self.data_type_config.get_generator_batch_size()
            batch_size = 100000
        
            generators = self.get_generators(batch_size, species_encoded)
    
            query_list = [
                [GeoXrefETL.geoXrefQuery, commit_size, "geoxref_data_" + sub_type.get_data_provider() + ".csv"],
            ]
            
            query_and_file_list = self.process_query_params(query_list)
            CSVTransactor.save_file_static(generators, query_and_file_list)
            Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, batch_size, species_encoded):
    
        entrezIds = []
        
        time.sleep(random.randint(0, 10)) # So that Geo wont fails on "TO MANY URL REQUESTS"
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?term=gene_geoprofiles[filter]+AND+%s[Organism]&retmax=%s&db=gene" % (species_encoded, batch_size)

        logger.info("Geo Url: " + url)

        geo_data_file_contents = Download("tmp", url, "geo").get_downloaded_data()
        geo_data = json.loads(json.dumps(xmltodict.parse(geo_data_file_contents)))

        for efetchKey, efetchValue in geo_data.items():
            # IdList is a value returned from efetch XML spec,
            # within IdList, there is another map with "Id" as the key and the entrez local ids a list value.
            for subMapKey, subMapValue in efetchValue.items():
                if subMapKey == 'IdList':
                    for idKey, idList in subMapValue.items():
                        for entrezId in idList:
                            # print ("here is the entrezid: " +entrezId)
                            entrezIds.append("NCBI_Gene:"+entrezId)

        query = "match (g:Gene)-[crr:CROSS_REFERENCE]-(cr:CrossReference) where cr.globalCrossRefId in {parameter} return g.primaryKey, g.modLocalId, cr.name, cr.globalCrossRefId"
        geo_data_list = []
        returnSet = Neo4jHelper.run_single_parameter_query(query, entrezIds)

        for record in returnSet:

            genePrimaryKey = record["g.primaryKey"]
            modLocalId = record["g.modLocalId"]
            globalCrossRefId = record["cr.globalCrossRefId"]
            geo_xref = ETLHelper.get_xref_dict(globalCrossRefId.split(":")[1], "NCBI_Gene", "gene/other_expression", "gene/other_expression", "GEO",
                                                     "https://www.ncbi.nlm.nih.gov/sites/entrez?Db=geoprofiles&DbFrom=gene&Cmd=Link&LinkName=gene_geoprofiles&LinkReadableName=GEO%20Profiles&IdsFromResult="+globalCrossRefId.split(":")[1],
                                                     globalCrossRefId+"gene/other_expression")
            
            geo_xref["genePrimaryKey"] = genePrimaryKey
            geo_xref["modLocalId"] = modLocalId

            geo_data_list.append(geo_xref)

        yield [geo_data_list]