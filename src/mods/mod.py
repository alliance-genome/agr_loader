from extractors import *
from files import S3File, TARFile, JSONFile
from etl.helpers import ETLHelper
from services import RetrieveGeoXrefService
from test import TestObject
import gzip, time
import csv
import os
import logging

logger = logging.getLogger(__name__)

class MOD(object):

    def __init__(self, batch_size, species):
        self.batch_size = batch_size
        self.species = species
        if "TEST_SET" in os.environ and os.environ['TEST_SET'] == "True":
            logger.warn("WARNING: Test data load enabled.")
            time.sleep(1)
            self.testObject = TestObject(True)
        else:
            self.testObject = TestObject(False)

    def extract_go_annots_mod(self, geneAssociationFileName, identifierPrefix):
        path = "tmp"
        S3File("GO/ANNOT/" + geneAssociationFileName, path).download()
        go_annot_dict = {}
        go_annot_list = []
        with gzip.open(path + "/GO/ANNOT/" + geneAssociationFileName, 'rt', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter='\t')
            for line in reader:
                if line[0].startswith('!'):
                    continue
                gene = identifierPrefix + line[1]
                go_id = line[4]
                dateProduced = line[14]
                dataProvider = line[15]
                qualifier = line[3]
                if not qualifier:
                    qualifier = ""
                if gene in go_annot_dict:
                    go_annot_dict[gene]['annotations'].append(
                        {"go_id": go_id, "evidence_code": line[6], "aspect": line[8], "qualifier": qualifier})
                else:
                    go_annot_dict[gene] = {
                        'gene_id': gene,
                        'annotations': [{"go_id": go_id, "evidence_code": line[6], "aspect": line[8],
                                         "qualifier": qualifier}],
                        'species': self.species,
                        'loadKey': dataProvider + "_" + dateProduced + "_" + "GAF",
                        'dataProvider': dataProvider,
                        'dateProduced': dateProduced,
                    }
        # Convert the dictionary into a list of dictionaries for Neo4j.
        # Check for the use of testObject and only return test data if necessary.
        if self.testObject.using_test_data() is True:
            for entry in go_annot_dict:
                if self.testObject.check_for_test_id_entry(go_annot_dict[entry]['gene_id']) is True:
                    go_annot_list.append(go_annot_dict[entry])
                    self.testObject.add_ontology_ids([annotation["go_id"] for annotation in
                                                 go_annot_dict[entry]['annotations']])
                else:
                    continue
            return go_annot_list
        else:
            for entry in go_annot_dict:
                go_annot_list.append(go_annot_dict[entry])
            return go_annot_list


    def load_wt_expression_objects_mod(self, expressionFileName, loadFile):
        data = WTExpressionExt().get_wt_expression_data(loadFile, expressionFileName, 10000, self.testObject)
        return data

    def extract_geo_entrez_ids_from_geo_mod(self, geoRetMax):
        entrezIds = []

        data = GeoExt().get_entrez_ids(self.species, "gene_geoprofiles", "gene", geoRetMax, "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?")

        for efetchKey, efetchValue in data.items():
            # IdList is a value returned from efetch XML spec,
            # within IdList, there is another map with "Id" as the key and the entrez local ids a list value.
            for subMapKey, subMapValue in efetchValue.items():
                if subMapKey == 'IdList':
                    for idKey, idList in subMapValue.items():
                        for entrezId in idList:
                            # print ("here is the entrezid: " +entrezId)
                            entrezIds.append("NCBI_Gene:"+entrezId)


        xrefs = self.get_geo_xref(entrezIds)

        return xrefs
    
    def get_geo_xref(self, global_id_list):

        query = "match (g:Gene)-[crr:CROSS_REFERENCE]-(cr:CrossReference) where cr.globalCrossRefId in {parameter} return g.primaryKey, g.modLocalId, cr.name, cr.globalCrossRefId"
        geo_data = []
        returnSet = Neo4jTransactor.run_single_parameter_query(query, global_id_list)
        
        counter = 0
        
        for record in returnSet:
            counter += 1
            genePrimaryKey = record["g.primaryKey"]
            modLocalId = record["g.modLocalId"]
            globalCrossRefId = record["cr.globalCrossRefId"]
            geo_xref = ETLHelper.get_xref_dict(globalCrossRefId.split(":")[1], "NCBI_Gene",
                                                     "gene/other_expression", "gene/other_expression", "GEO",
                                                     "https://www.ncbi.nlm.nih.gov/sites/entrez?Db=geoprofiles&DbFrom=gene&Cmd=Link&LinkName=gene_geoprofiles&LinkReadableName=GEO%20Profiles&IdsFromResult="+globalCrossRefId.split(":")[1],
                                                     globalCrossRefId+"gene/other_expression")
            geo_xref["genePrimaryKey"] = genePrimaryKey
            geo_xref["modLocalId"] = modLocalId

            geo_data.append(geo_xref)

        return geo_data


