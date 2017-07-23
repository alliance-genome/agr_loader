from extractors.bgi_ext import BGIExt
from extractors.disease_ext import DiseaseExt
from files import *
import gzip
import csv

class MOD():

    def load_genes(self, batch_size, test_set, bgiName, loadFile):
        path = "tmp"
        S3File("mod-datadumps", self.loadFile, path).download()
        TARFile(path, self.loadFile).extract_all()
        gene_data = JSONFile().get_data(path + bgiName)
        gene_lists = BGIExt().get_data(gene_data, batch_size, test_set)
        return self.yield_gene_lists(gene_lists)

    def yield_gene_lists(self, gene_lists):
        yield from gene_lists

    def load_go(self, geneAssociationFile, species):
        path = "tmp"
        S3File("mod-datadumps/GO/ANNOT", geneAssociationFile, path).download()
        go_annot_dict = {}
        with gzip.open(path + "/gene_association.fb.gz", 'rb') as file:
            reader = csv.reader(file, delimiter='\t')
            for line in reader:
                if line[0].startswith('!'):
                    continue
                gene = line[1]
                go_id = line[4]
                if gene in go_annot_dict:
                    go_annot_dict[gene]['go_id'].append(go_id)
                else:
                    go_annot_dict[gene] = {
                        'gene_id': gene,
                        'go_id': [go_id],
                        'species': species
                    }
        return go_annot_dict

    def load_do_annots(self, diseaseName):
        path = "tmp"
        S3File("mod-datadumps", self.loadFile, path).download()
        TARFile(path, self.loadFile).extract_all()
        disease_data = JSONFile().get_data(path + diseaseName)
        gene_disease_dict = DiseaseExt().get_data(disease_data)

        return gene_disease_dict