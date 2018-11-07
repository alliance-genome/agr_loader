from extractors import *
from files import S3File, TARFile, JSONFile
from services import RetrieveGeoXrefService
from test import TestObject
import gzip
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


    def load_disease_gene_objects_mod(self, diseaseFileName, loadFile):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        disease_data = JSONFile().get_data(path + diseaseFileName, 'disease')
        disease_dict = DiseaseGeneExt().get_gene_disease_data(disease_data, self.batch_size, self.species)

        return disease_dict

    def load_disease_allele_objects_mod(self, diseaseFileName, loadFile):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        disease_data = JSONFile().get_data(path + diseaseFileName, 'disease')
        disease_dict = DiseaseAlleleExt().get_allele_disease_data(disease_data, self.batch_size, self.species)
        return disease_dict

    def load_allele_objects_mod(self, alleleFileName, loadFile):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        alleleData = JSONFile().get_data(path + alleleFileName, 'allele')
        alleleDict = AlleleExt().get_alleles(alleleData, self.batch_size, self.testObject, self.species)
        return alleleDict

    def load_phenotype_objects_mod(self, phenotypeFileName, loadFile):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        phenotype_data = JSONFile().get_data(path + phenotypeFileName, 'phenotype')
        phenotype_dict = PhenotypeExt().get_phenotype_data(phenotype_data, self.batch_size, self.testObject, self.species)
        return phenotype_dict

    def load_genes_mod(self, bgiFileName, loadFile):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        gene_data = JSONFile().get_data(path + bgiFileName, 'BGI')
        gene_lists = BGIExt().get_data(gene_data, self.batch_size, self.testObject, self.species)
        return gene_lists

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


        xrefs = RetrieveGeoXrefService().get_geo_xref(entrezIds)

        return xrefs

    def extract_ortho_data_mod(self, mod_name):
        data = OrthoExt().get_data(self.testObject, mod_name, 20000) # generator object
        return data
