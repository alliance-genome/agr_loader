from .disease_ext import get_disease_record
from .primary_data_object_type import PrimaryDataObjectType
from loaders.transactions import Transaction
from services import SpeciesService
from services import UrlService
from services import CreateCrossReference
from .resource_descriptor_ext import ResourceDescriptor

class DiseaseGeneExt(object):

    def get_gene_disease_data(self, disease_data, batch_size):
        list_to_yield = []
        dateProduced = disease_data['metaData']['dateProduced']
        release = None

        xrefUrlMap = ResourceDescriptor().get_data()

        for dataProviderObject in disease_data['metaData']['dataProvider']:

            dataProviderCrossRef = dataProviderObject.get('crossReference')
            dataProviderType = dataProviderObject.get('type')
            dataProvider = dataProviderCrossRef.get('id')
            dataProviderPages = dataProviderCrossRef.get('pages')
            dataProviderCrossRefSet = []
            dataProviders = []
            loadKey = dateProduced + "_BGI"

            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, xrefUrlMap, dataProvider,
                                                                       dataProviderPage)
                dataProviderCrossRefSet.append(
                    CreateCrossReference.get_xref(dataProvider, dataProvider, dataProviderPage,
                                                  dataProviderPage, dataProvider, crossRefCompleteUrl,
                                                  dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                loadKey = dataProvider + loadKey


        if 'release' in disease_data['metaData']:
            release = disease_data['metaData']['release']

            for diseaseRecord in disease_data['data']:

                diseaseObjectType = diseaseRecord['objectRelation'].get("objectType")

                if diseaseObjectType != PrimaryDataObjectType.gene.name:
                        continue
                else:
                    #TODO:fix this dependency - should be no need for allelicGeneId here.
                    allelicGeneId = ''
                    disease_features = get_disease_record(diseaseRecord, dataProviders, dateProduced, release, allelicGeneId)

                    list_to_yield.append(disease_features)
                    if len(list_to_yield) == batch_size:
                        yield list_to_yield

                        list_to_yield[:] = []  # Empty the list.

            if len(list_to_yield) > 0:
                yield list_to_yield

