from extractors.bgi_ext import BGIExt
from extractors.disease_ext import DiseaseExt
from files import *
import gzip
import csv

class MOD(object):

    def load_genes(self, batch_size, testObject, bgiName, loadFile):
        path = "tmp"
        S3File("mod-datadumps", loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        gene_data = JSONFile().get_data(path + bgiName)
        gene_lists = BGIExt().get_data(gene_data, batch_size, testObject)
        return self.yield_gene_lists(gene_lists)

    def yield_gene_lists(self, gene_lists):
        yield from gene_lists

    def extract_go_annots(self, geneAssociationFile, species, identifierPrefix, testObject):
        path = "tmp"
        S3File("mod-datadumps/GO/ANNOT", geneAssociationFile, path).download()
        go_annot_dict = {}
        go_annot_list = []
        with gzip.open(path + "/" + geneAssociationFile, 'rt') as file:
            reader = csv.reader(file, delimiter='\t')
            if species == "Homo sapiens": # Special case for human GO annotations.
                for row in reader:
                gene = row[0]
                go_terms = map(lambda s: s.strip(), row[1].split(","))
                for term in go_terms:
                    if gene in go_annot_dict:
                        go_annot_dict[gene]['go_id'].append(go_id)
                    else:
                        go_annot_dict[gene] = {
                            'gene_id': gene,
                            'go_id': [go_id],
                            'species': species
                        }
            else:
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
        # Check for the use of testObject and only return test data if necessary.
        if testObject.using_test_data() == True:
            for entry in go_annot_dict:
                if testObject.check_for_test_id_entry(go_annot_dict[entry]['gene_id']) == True:
                    go_annot_list.append(go_annot_dict[entry])
                    testObject.add_go_ids(go_annot_dict[entry]['go_id'])
                else:
                    continue
            return go_annot_list
        else:
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

    def load_disease_objects(self, batch_size, testObject, diseaseName, loadFile):
        path = "tmp"
        S3File("mod-datadumps", self.loadFile, path).download()
        TARFile(path, self.loadFile).extract_all()
        disease_data = JSONFile().get_data(path + diseaseName)
        disease_dict = DiseaseExt().get_features(disease_data, batch_size, testObject)
        #print (disease_dict)
        return disease_dict