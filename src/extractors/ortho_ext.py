from files import S3File, TARFile, JSONFile
from .id_ext import IdExt
import uuid
from services import UrlService
from services import CreateCrossReference
from .resource_descriptor_ext import ResourceDescriptor

class OrthoExt(object):

    @staticmethod
    def get_data(testObject, mod_name, batch_size):

        xrefUrlMap = ResourceDescriptor().get_data()
        path = "tmp"

        if testObject.using_test_data() is True:
            filename = 'orthology_test_data_1.0.0.3.json'
            filename_comp = 'ORTHO/orthology_test_data_1.0.0.3.json.tar.gz'
        else:
            filename = "orthology_" + mod_name + "_1.0.0.3.json"
            filename_comp = "ORTHO/orthology_" + mod_name + "_1.0.0.3.json.tar.gz"

        S3File(filename_comp, path).download()
        TARFile(path, filename_comp).extract_all()
        ortho_data = JSONFile().get_data(path + "/" + filename, 'orthology')

        list_to_yield = []

        for orthoRecord in ortho_data['data']:

            for dataProviderObject in ortho_data['metaData']['dataProvider']:

                dataProviderCrossRef = dataProviderObject.get('crossReference')
                dataProviderType = dataProviderObject.get('type')
                dataProvider = dataProviderCrossRef.get('id')
                dataProviderPages = dataProviderCrossRef.get('pages')
                dataProviderCrossRefSet = []

                for dataProviderPage in dataProviderPages:
                    crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, xrefUrlMap, dataProvider,
                                                                           dataProviderPage)
                    dataProviderCrossRefSet.append(
                        CreateCrossReference.get_xref(dataProvider, dataProvider, dataProviderPage,
                                                      dataProviderPage, dataProvider, crossRefCompleteUrl,
                                                      dataProvider + dataProviderPage))

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
                        'gene2AgrPrimaryId': gene2AgrPrimaryId,

                        'matched': orthoRecord['predictionMethodsMatched'],
                        'notMatched': orthoRecord['predictionMethodsNotMatched'],
                        'notCalled': orthoRecord['predictionMethodsNotCalled'],

                        'confidence': orthoRecord['confidence'],

                        'uuid': str(uuid.uuid4())
                    }

                # Establishes the number of entries to yield (return) at a time.
                list_to_yield.append(ortho_dataset)
                if len(list_to_yield) == batch_size:
                    yield list_to_yield
                    list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield
