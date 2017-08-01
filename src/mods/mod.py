from extractors.bgi_ext import BGIExt
from extractors.disease_ext import DiseaseExt
from files import *
import gzip
import csv

class MOD(object):

    def load_genes(self, batch_size, test_set, bgiName, loadFile):
        path = "tmp"
        S3File("mod-datadumps", loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        gene_data = JSONFile().get_data(path + bgiName)
        gene_lists = BGIExt().get_data(gene_data, batch_size, test_set)
        return self.yield_gene_lists(gene_lists)

    def yield_gene_lists(self, gene_lists):
        yield from gene_lists

    def load_go_annots(self, geneAssociationFile, species, identifierPrefix):
        path = "tmp"
        S3File("mod-datadumps/GO/ANNOT", geneAssociationFile, path).download()
        go_annot_dict = {}
        go_annot_list = []
        with gzip.open(path + "/" + geneAssociationFile, mode='rt') as file:
            reader = csv.reader(file, delimiter='\t')
            for line in reader:
                if line[0].startswith('!'):
                    continue
                gene = identifierPrefix + line[1]
                go_id = line[4]
                if gene in go_annot_dict:
                    go_annot_dict[gene]['go_id'].append(go_id)
                else:
                    go_annot_dict[gene] = {
                        'gene_id': gene,
                        'go_id': [go_id],
                        'species': species
                    }
        # Convert the dictionary into a list of dictionaries for Neo4j.
        for entry in go_annot_dict:
            go_annot_list.append(go_annot_dict[entry])
        return go_annot_list

    def load_go_annots_prefix(self, geneAssociationFile, species, identifierPrefix):
        path = "tmp"
        S3File("mod-datadumps/GO/ANNOT", geneAssociationFile, path).download()
        go_annot_dict = {}
        go_annot_list = []
        with gzip.open(path + "/" + geneAssociationFile, 'rt') as file:
            reader = csv.reader(file, delimiter='\t')
            for line in reader:
                if line[0].startswith('!'):
                    continue
                gene = line[0] + ":" + line[1]
                go_id = line[4]
                prefix = line[0]
                if gene in go_annot_dict:
                    go_annot_dict[gene]['go_id'].append(go_id)
                else:
                    go_annot_dict[gene] = {
                        'gene_id': gene,
                        'go_id': [go_id],
                        'species': species,
                        'prefix': prefix
                    }
        # Convert the dictionary into a list of dictionaries for Neo4j.
        for entry in go_annot_dict:
            go_annot_list.append(go_annot_dict[entry])
        return go_annot_list

    def load_go_annots_human(self, geneAssociationFile, species, identifierPrefix):
        path = "tmp"
        S3File("mod-datadumps/GO/ANNOT", geneAssociationFile, path).download()
        go_annot_dict = {}
        go_annot_list = []
        with gzip.open(path + "/" + geneAssociationFile, 'rt') as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                gene = row[0]
                go_terms = map(lambda s: s.strip(), row[1].split(","))
                for term in go_terms:
                    if gene in go_annot_dict:
                        go_annot_dict[gene]['go_id'].append(term)
                    else:
                        go_annot_dict[gene] = {
                            'gene_id': gene,
                            'go_id': [term],
                            'species': species
                        }
        # Convert the dictionary into a list of dictionaries for Neo4j.
        for entry in go_annot_dict:
            go_annot_list.append(go_annot_dict[entry])
        return go_annot_list

    def load_do_annots(self, diseaseName):
        path = "tmp"
        S3File("mod-datadumps", self.loadFile, path).download()
        TARFile(path, self.loadFile).extract_all()
        disease_data = JSONFile().get_data(path + diseaseName)
        gene_disease_dict = DiseaseExt().get_data(disease_data)

        return gene_disease_dict