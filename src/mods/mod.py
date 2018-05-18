from extractors.bgi_ext import BGIExt
from extractors.disease_gene_ext import DiseaseGeneExt
from extractors.disease_allele_ext import DiseaseAlleleExt
from extractors.allele_ext import AlleleExt
from extractors.geo_ext import GeoExt
from extractors.phenotype_ext import PhenotypeExt
from files import S3File, TARFile, JSONFile
from services import RetrieveGeoXrefService
import uuid
import gzip
import csv
import json
import pprint

class MOD(object):

    def load_genes_mod(self, batch_size, testObject, bgiName, loadFile):
        path = "tmp"
        S3File("mod-datadumps", loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        gene_data = JSONFile().get_data(path + bgiName, 'BGI')
        gene_lists = BGIExt().get_data(gene_data, batch_size, testObject)
        return self.yield_gene_lists(gene_lists)

    def yield_gene_lists(self, gene_lists):
        yield from gene_lists

    def extract_go_annots_mod(self, geneAssociationFile, species, identifierPrefix, testObject):
        path = "tmp"
        S3File("mod-datadumps/GO/ANNOT", geneAssociationFile, path).download()
        go_annot_dict = {}
        go_annot_list = []
        with gzip.open(path + "/" + geneAssociationFile, 'rt') as file:
            reader = csv.reader(file, delimiter='\t')
            for line in reader:
                if line[0].startswith('!'):
                    continue
                gene = identifierPrefix + line[1]
                go_id = line[4]
                dateProduced = line[14]
                dataProvider = line[15]
                if gene in go_annot_dict:
                    go_annot_dict[gene]['go_id'].append(go_id)
                else:
                    go_annot_dict[gene] = {
                        'gene_id': gene,
                        'go_id': [go_id],
                        'species': species,
                        'loadKey': dataProvider + "_" + dateProduced + "_" + "GAF",
                        'dataProvider': dataProvider,
                        'dateProduced': dateProduced
                    }
        # Convert the dictionary into a list of dictionaries for Neo4j.
        # Check for the use of testObject and only return test data if necessary.
        if testObject.using_test_data() is True:
            for entry in go_annot_dict:
                if testObject.check_for_test_id_entry(go_annot_dict[entry]['gene_id']) is True:
                    go_annot_list.append(go_annot_dict[entry])
                    testObject.add_ontology_ids(go_annot_dict[entry]['go_id'])
                else:
                    continue
            return go_annot_list
        else:
            for entry in go_annot_dict:
                go_annot_list.append(go_annot_dict[entry])
            return go_annot_list


    def load_disease_gene_objects_mod(self,batch_size, testObject, diseaseName, loadFile):
        path = "tmp"
        S3File("mod-datadumps", loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        disease_data = JSONFile().get_data(path + diseaseName, 'disease')
        disease_dict = DiseaseGeneExt().get_gene_disease_data(disease_data, batch_size)

        return disease_dict

    def load_disease_allele_objects_mod(self, batch_size, testObject, diseaseName, loadFile, graph):
        path = "tmp"
        S3File("mod-datadumps", loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        disease_data = JSONFile().get_data(path + diseaseName, 'disease')
        disease_dict = DiseaseAlleleExt().get_allele_disease_data(disease_data, batch_size, graph)

        return disease_dict

    def load_phenotype_objects_mod(self, batch_size, testObject, phenotypeName, loadFile, graph):
        path = "tmp"
        S3File("mod-datadumps", loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        phenotype_data = JSONFile().get_data(path + phenotypeName, 'phenotype')
        phenotype_dict = PhenotypeExt.get_phenotype_data(phenotype_data, batch_size, graph)

        return phenotype_dict

    def load_allele_objects_mod(self, batch_size, testObject, alleleName, loadFile, graph):
        path = "tmp"
        S3File("mod-datadumps", loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        alleleData = JSONFile().get_data(path + alleleName, 'allele')
        alleleDict = AlleleExt().get_alleles(alleleData, batch_size, testObject, graph)

        return alleleDict

    def extract_geo_entrez_ids_from_geo(self, geoSpecies, geoRetMax, graph):
        entrezIds = []
        xrefs = []
        geoTerm = "gene_geoprofiles"
        geoDb = "gene"
        geoRetrievalUrlPrefix = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?"

        data = GeoExt().get_entrez_ids(geoSpecies, geoTerm, geoDb, geoRetMax, geoRetrievalUrlPrefix)

        for efetchKey, efetchValue in data.items():
            # IdList is a value returned from efetch XML spec,
            # within IdList, there is another map with "Id" as the key and the entrez local ids a list value.
            for subMapKey, subMapValue in efetchValue.items():
                if subMapKey == 'IdList':
                    for idKey, idList in subMapValue.items():
                        for entrezId in idList:
                            # print ("here is the entrezid: " +entrezId)
                            entrezIds.append("NCBI_Gene:"+entrezId)


        xrefs = RetrieveGeoXrefService().get_geo_xref(entrezIds, graph)

        return xrefs
