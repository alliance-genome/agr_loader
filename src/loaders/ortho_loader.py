from files import *
from loaders import *
import time
import gc
import json
from test_check import check_for_test_entry

class OrthoLoader:

    @staticmethod
    def get_data(mod_name, test_set, gene_master_set):
        path = "tmp"
        filename = None
        filename_comp = None
        if test_set == 'true':
            filename = '/orthology_test_data_0.6.1_3.json'
            filename_comp = 'orthology_test_data_0.6.1_3.json.tar.gz'
        else:
            filename = "/orthology_" + mod_name + "_0.6.1_3.json"
            filename_comp = "orthology_" + mod_name + "_0.6.1_3.json.tar.gz"

        S3File("mod-datadumps/ORTHO", filename_comp, path).download()
        TARFile(path, filename_comp).extract_all()
        ortho_data = JSONFile().get_data(path + filename)

        ortho_dataset = {}

        dateProduced = ortho_data['metaData']['dateProduced']
        dataProvider = ortho_data['metaData']['dataProvider']
        release = None

        if 'release' in ortho_data['metaData']:
            release = ortho_data['metaData']['release']

        for orthoRecord in ortho_data['data']:
            ortho_entry = {}

            # Sort out identifiers and prefixes.
            gene1 = IdLoader().process_identifiers(orthoRecord['gene1'], dataProvider) # 'DRSC:'' removed, local ID, functions as display ID.
            gene2 = IdLoader().process_identifiers(orthoRecord['gene2'], dataProvider) # 'DRSC:'' removed, local ID, functions as display ID.

            gene1Species = orthoRecord['gene1Species']
            gene2Species = orthoRecord['gene2Species']

            gene1AgrPrimaryId = IdLoader().add_agr_prefix_by_species(gene1, gene1Species) # Prefixed according to AGR prefixes.
            gene2AgrPrimaryId = IdLoader().add_agr_prefix_by_species(gene2, gene2Species) # Prefixed according to AGR prefixes.

            if gene1AgrPrimaryId not in ortho_dataset:
                ortho_dataset[gene1AgrPrimaryId] = []
            if gene2AgrPrimaryId not in gene_master_set:
                continue # Skip entries where we don't have gene 2 in AGR.
            ortho_dataset[gene1AgrPrimaryId].append({
                'isBestScore': orthoRecord['isBestScore'],
                'isBestRevScore': orthoRecord['isBestRevScore'],

                'gene1Species': gene1Species,
                'gene1SpeciesName': orthoRecord['gene1SpeciesName'],

                'gene2AgrPrimaryId': gene2AgrPrimaryId,
                'gene2Symbol' : orthoRecord['gene2Symbol'],
                'gene2Species': gene2Species,
                'gene2SpeciesName': orthoRecord['gene2SpeciesName'],

                'predictionMethodsMatched': orthoRecord['predictionMethodsMatched'],
                'predictionMethodsNotMatched': orthoRecord['predictionMethodsNotMatched'],
                'predictionMethodsNotCalled': orthoRecord['predictionMethodsNotCalled'],

                'confidence': orthoRecord['confidence']
            })

        del ortho_data
        return ortho_dataset