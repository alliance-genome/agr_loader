from .disease_ext import get_disease_record
from .primary_data_object_type import PrimaryDataObjectType

class DiseaseGeneExt(object):

    def get_gene_disease_data(self, disease_data, batch_size):
        list_to_yield = []
        dateProduced = disease_data['metaData']['dateProduced']
        dataProvider = disease_data['metaData']['dataProvider']
        release = None

        if 'release' in disease_data['metaData']:
            release = disease_data['metaData']['release']

        for diseaseRecord in disease_data['data']:

            diseaseObjectType = diseaseRecord['objectRelation'].get("objectType")

            if diseaseObjectType != PrimaryDataObjectType.gene.name:
                continue
            else:
                #TODO:fix this dependency - should be no need for allelicGeneId here.
                allelicGeneId = ''
                disease_features = get_disease_record(diseaseRecord, dataProvider, dateProduced, release, allelicGeneId)

                list_to_yield.append(disease_features)
                if len(list_to_yield) == batch_size:
                    yield list_to_yield

                    list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield
