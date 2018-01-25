from .disease_ext import get_disease_record


class DiseaseGeneExt(object):

    def get_gene_disease_data(self, disease_data, batch_size):
        disease_features = {}
        list_to_yield = []
        dateProduced = disease_data['metaData']['dateProduced']
        dataProvider = disease_data['metaData']['dataProvider']
        release = None

        if 'release' in disease_data['metaData']:
            release = disease_data['metaData']['release']

        for diseaseRecord in disease_data['data']:

            diseaseObjectType = diseaseRecord['objectRelation'].get("objectType")

            if diseaseObjectType == 'allele':
                continue
            elif diseaseObjectType == 'strain':
                continue
            elif diseaseObjectType == 'fish':
                continue
            elif diseaseObjectType == 'genotype':
                continue
            else:

                disease_features = get_disease_record(diseaseRecord, dataProvider, dateProduced, release)

                list_to_yield.append(disease_features)
                if len(list_to_yield) == batch_size:
                    yield list_to_yield

                    list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield
