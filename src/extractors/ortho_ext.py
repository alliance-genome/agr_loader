from files import S3File, TARFile, JSONFile
from .id_ext import IdExt
from services import UrlService
from services import CreateCrossReference
from .resource_descriptor_ext import ResourceDescriptor
import uuid

class OrthoExt(object):

    @staticmethod
    def get_data(testObject, mod_name, batch_size):
        path = "tmp"
        if testObject.using_test_data() is True:
            filename = 'orthology_test_data_1.0.0.7_temp1.json'
            filename_comp = 'ORTHO/orthology_test_data_1.0.0.7_temp1.json.tar.gz'
        else:
            filename = "orthology_" + mod_name + "_1.0.0.7_temp.json"
            filename_comp = "ORTHO/orthology_" + mod_name + "_1.0.0.7_temp.json.tar.gz"

        S3File(filename_comp, path).download()
        TARFile(path, filename_comp).extract_all()
        ortho_data = JSONFile().get_data(path + "/" + filename, 'orthology')
        counter = 0
        matched_data = []
        unmatched_data = []
        ortho_data_list = []
        notcalled_data = []

        xrefUrlMap = ResourceDescriptor().get_data()

        dataProviderObject = ortho_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []
        dataProviders = []

        for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, xrefUrlMap, dataProvider,
                                                                       dataProviderPage)
                dataProviderCrossRefSet.append(
                    CreateCrossReference.get_xref(dataProvider, dataProvider, dataProviderPage,
                                                  dataProviderPage, dataProvider, crossRefCompleteUrl,
                                                  dataProvider + dataProviderPage))

        dataProviders.append(dataProvider)

        for orthoRecord in ortho_data['data']:
            counter = counter + 1
            # Sort out identifiers and prefixes.
            gene1 = IdExt().process_identifiers(orthoRecord['gene1'], dataProviders) # 'DRSC:'' removed, local ID, functions as display ID.
            gene2 = IdExt().process_identifiers(orthoRecord['gene2'], dataProviders) # 'DRSC:'' removed, local ID, functions as display ID.

            gene1Species = orthoRecord['gene1Species']
            gene2Species = orthoRecord['gene2Species']

            gene1AgrPrimaryId = IdExt().add_agr_prefix_by_species(gene1, gene1Species) # Prefixed according to AGR prefixes.
            gene2AgrPrimaryId = IdExt().add_agr_prefix_by_species(gene2, gene2Species) # Prefixed according to AGR prefixes.
            ortho_uuid = str(uuid.uuid4())

            if gene1AgrPrimaryId is not None and gene2AgrPrimaryId is not None:

                ortho_dataset = {
                    'isBestScore': orthoRecord['isBestScore'],
                    'isBestRevScore': orthoRecord['isBestRevScore'],

                    'gene1AgrPrimaryId': gene1AgrPrimaryId,
                    'gene2AgrPrimaryId': gene2AgrPrimaryId,

                    'confidence': orthoRecord['confidence'],

                    'strictFilter': orthoRecord['strictFilter'],
                    'moderateFilter': orthoRecord['moderateFilter'],
                    'uuid': ortho_uuid
                }
                ortho_data_list.append(ortho_dataset)

                for matched in orthoRecord.get('predictionMethodsMatched'):
                    matched_dataset = {
                        "uuid": ortho_uuid,
                        "algorithm": matched
                    }
                    matched_data.append(matched_dataset)

                for unmatched in orthoRecord.get('predictionMethodsNotMatched'):
                    unmatched_dataset = {
                        "uuid": ortho_uuid,
                        "algorithm": unmatched
                    }
                    unmatched_data.append(unmatched_dataset)

                for notcalled in orthoRecord.get('predictionMethodsNotCalled'):
                    notcalled_dataset = {
                        "uuid": ortho_uuid,
                        "algorithm": notcalled
                    }
                    notcalled_data.append(notcalled_dataset)

                # Establishes the number of entries to yield (return) at a time.
                if counter == batch_size:
                    yield (ortho_data_list, matched_data, unmatched_data, notcalled_data)
                    ortho_data_list = []
                    matched_data = []
                    unmatched_data = []
                    notcalled_data = []
                    counter = 0

        if counter > 0:
            yield (ortho_data_list, matched_data, unmatched_data, notcalled_data)
