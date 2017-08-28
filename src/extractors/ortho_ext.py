from files import *
from extractors import *
import time
import gc
import json
import uuid
from test import *

class OrthoExt:

    @staticmethod
    def get_data(testObject, mod_name, batch_size):
        path = "tmp"
        filename = None
        filename_comp = None
        if testObject.using_test_data() == True:
            filename = '/orthology_test_data_0.6.1_3.json'
            filename_comp = 'orthology_test_data_0.6.1_3.json.tar.gz'
        else:
            filename = "/orthology_" + mod_name + "_0.6.1_3.json"
            filename_comp = "orthology_" + mod_name + "_0.6.1_3.json.tar.gz"

        S3File("mod-datadumps/ORTHO", filename_comp, path).download()
        TARFile(path, filename_comp).extract_all()
        ortho_data = JSONFile().get_data(path + filename)

        dateProduced = ortho_data['metaData']['dateProduced']
        dataProvider = ortho_data['metaData']['dataProvider']
        release = None

        if 'release' in ortho_data['metaData']:
            release = ortho_data['metaData']['release']

        list_to_yield = []

        for orthoRecord in ortho_data['data']:

            # Sort out identifiers and prefixes.
            gene1 = IdExt().process_identifiers(orthoRecord['gene1'], dataProvider) # 'DRSC:'' removed, local ID, functions as display ID.
            gene2 = IdExt().process_identifiers(orthoRecord['gene2'], dataProvider) # 'DRSC:'' removed, local ID, functions as display ID.

            gene1Species = orthoRecord['gene1Species']
            gene2Species = orthoRecord['gene2Species']

            gene1AgrPrimaryId = IdExt().add_agr_prefix_by_species(gene1, gene1Species) # Prefixed according to AGR prefixes.
            gene2AgrPrimaryId = IdExt().add_agr_prefix_by_species(gene2, gene2Species) # Prefixed according to AGR prefixes.

            if gene1AgrPrimaryId is not None and gene2AgrPrimaryId is not None:

                ortho_dataset = {
                    'isBestScore': orthoRecord['isBestScore'],
                    'isBestRevScore': orthoRecord['isBestRevScore'],

                    'gene1AgrPrimaryId' : gene1AgrPrimaryId,
                    # 'gene1Species': gene1Species,
                    # 'gene1SpeciesName': orthoRecord['gene1SpeciesName'],

                    'gene2AgrPrimaryId': gene2AgrPrimaryId,
                    # 'gene2Symbol' : orthoRecord['gene2Symbol'],
                    # 'gene2Species': gene2Species,
                    # 'gene2SpeciesName': orthoRecord['gene2SpeciesName'],

                    'matched': orthoRecord['predictionMethodsMatched'],
                    'notMatched': orthoRecord['predictionMethodsNotMatched'],
                    'notCalled': orthoRecord['predictionMethodsNotCalled'],

                    'confidence': orthoRecord['confidence'],

                    'uuid': str(uuid.uuid1())
                }

                # Establishes the number of entries to yield (return) at a time.
                list_to_yield.append(ortho_dataset)
                if len(list_to_yield) == batch_size:
                    yield list_to_yield
                    list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield