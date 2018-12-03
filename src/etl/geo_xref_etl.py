import json
import logging, uuid, urllib, xmltodict

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from files import JSONFile, Download
from transactors import CSVTransactor

logger = logging.getLogger(__name__)



class GeoXrefETL(ETL):

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MERGE (n:Node {primaryKey:row.id})
            SET n.name = row.name """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):



        filepath = self.data_type_config.get_single_filepath()

        commit_size = self.data_type_config.get_neo4j_commit_size()
        #batch_size = self.data_type_config.get_generator_batch_size()
        batch_size = 100000
        
        generators = self.get_generators(filepath, batch_size)

        query_list = [
            [GeoXrefETL.query_template, commit_size, "stub_data.csv"],
        ]
            
        CSVTransactor.execute_transaction(generators, query_list)

    def get_generators(self, batch_size):
    
        entrezIds = []

        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?term=gene_geoprofiles[filter]+AND+" + urllib.parse.quote_plus(self.species) + "[Organism]&retmax=" + batch_size + "&db=gene"

        logger.info ("efetch url: " + url)

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