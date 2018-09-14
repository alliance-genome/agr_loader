from extractors.bgi_ext import BGIExt
from extractors.disease_gene_ext import DiseaseGeneExt
from extractors.disease_allele_ext import DiseaseAlleleExt
from extractors.allele_ext import AlleleExt
from extractors.wt_expression_ext import WTExpressionExt
from extractors.geo_ext import GeoExt
from extractors.phenotype_ext import PhenotypeExt
from files import S3File, TARFile, JSONFile
from services import RetrieveGeoXrefService
import gzip
import csv
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MOD(object):

    def extract_go_annots_mod(self, geneAssociationFile, species, identifierPrefix, testObject):
        path = "tmp"
        S3File("GO/ANNOT/" + geneAssociationFile, path).download()
        go_annot_dict = {}
        go_annot_list = []
        with gzip.open(path + "/GO/ANNOT/" + geneAssociationFile, 'rt', encoding='utf-8') as file:
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
                        'species': species,
                        'loadKey': dataProvider + "_" + dateProduced + "_" + "GAF",
                        'dataProvider': dataProvider,
                        'dateProduced': dateProduced,
                    }
        # Convert the dictionary into a list of dictionaries for Neo4j.
        # Check for the use of testObject and only return test data if necessary.
        if testObject.using_test_data() is True:
            for entry in go_annot_dict:
                if testObject.check_for_test_id_entry(go_annot_dict[entry]['gene_id']) is True:
                    go_annot_list.append(go_annot_dict[entry])
                    testObject.add_ontology_ids([annotation["go_id"] for annotation in
                                                 go_annot_dict[entry]['annotations']])
                else:
                    continue
            return go_annot_list
        else:
            for entry in go_annot_dict:
                go_annot_list.append(go_annot_dict[entry])
            return go_annot_list


    def load_disease_gene_objects_mod(self,batch_size, testObject, diseaseName, loadFile, species):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        disease_data = JSONFile().get_data(path + diseaseName, 'disease')
        disease_dict = DiseaseGeneExt().get_gene_disease_data(disease_data, batch_size, species)

        return disease_dict

    def load_disease_allele_objects_mod(self, batch_size, testObject, diseaseName, loadFile, graph, species):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        disease_data = JSONFile().get_data(path + diseaseName, 'disease')
        disease_dict = DiseaseAlleleExt().get_allele_disease_data(disease_data, batch_size, graph, species)

        return disease_dict

    def load_allele_objects_mod(self, batch_size, testObject, alleleName, loadFile, species):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        alleleData = JSONFile().get_data(path + alleleName, 'allele')
        alleleDict = AlleleExt().get_alleles(alleleData, batch_size, testObject, species)
        logger.info(alleleDict)
        return alleleDict

    def load_phenotype_objects_mod(self, batch_size, testObject, phenotypeName, loadFile, species):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        phenotype_data = JSONFile().get_data(path + phenotypeName, 'phenotype')
        phenotype_dict = PhenotypeExt().get_phenotype_data(phenotype_data, batch_size, testObject, species)

        return phenotype_dict

    @classmethod
    def load_genes_mod(self, batch_size, testObject, bgiName, loadFile, species):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        gene_data = JSONFile().get_data(path + bgiName, 'BGI')
        gene_lists = BGIExt().get_data(gene_data, batch_size, testObject, species)
        return gene_lists

    def load_wt_expression_objects_mod(self, batch_size, testObject, expressionName, loadFile):

        data = WTExpressionExt().get_wt_expression_data(loadFile, expressionName, batch_size, testObject)
        return data

    def extract_geo_entrez_ids_from_geo(self, geoSpecies, geoRetMax, graph):
        entrezIds = []
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
