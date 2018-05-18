from .disease_ext import get_disease_record
from .primary_data_object_type import PrimaryDataObjectType

class PhenotypeGeneExt(object):

    def get_gene_phenotype_data(self, phenotype_data, batch_size):
        list_to_yield = []
        dateProduced = phenotype_data['metaData']['dateProduced']
        dataProvider = phenotype_data['metaData']['dataProvider']
        release = None

        if 'release' in phenotype_data['metaData']:
            release = phenotype_data['metaData']['release']

        for phenotypeRecord in phenotype_data['data']:

            phenotypeObjectType = phenotypeRecord['objectRelation'].get("objectType")

            if phenotypeObjectType != PrimaryDataObjectType.gene.name:
                continue
            else:
                #TODO:fix this dependency - should be no need for allelicGeneId here.
                allelicGeneId = ''
                phenotype_features = get_disease_record(phenotypeRecord, dataProvider, dateProduced, release, allelicGeneId)

                list_to_yield.append(phenotype_features)
                if len(list_to_yield) == batch_size:
                    yield list_to_yield

                    list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield