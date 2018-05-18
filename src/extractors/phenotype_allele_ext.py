from .phenotype_ext import get_phenotype_record
from .primary_data_object_type import PrimaryDataObjectType
from loaders.transactions import Transaction


class PhenotypeAlleleExt(object):

    def get_allele_phenotype_data(self, phenotype_data, batch_size, graph):
        list_to_yield = []
        dateProduced = phenotype_data['metaData']['dateProduced']
        dataProvider = phenotype_data['metaData']['dataProvider']
        release = None

        if 'release' in phenotype_data['metaData']:
            release = phenotype_data['metaData']['release']

        for phenotypeRecord in phenotype_data['data']:

            phenotypeObjectType = phenotypeRecord['objectRelation'].get("objectType")

            if phenotypeObjectType != PrimaryDataObjectType.allele.name:
                continue
            else:
                query = "match (g:Gene)-[]-(f:Feature) where f.primaryKey = {parameter} return g.primaryKey"
                featurePrimaryId = phenotypeRecord.get('objectId')
                tx = Transaction(graph)
                returnSet = tx.run_single_parameter_query(query, featurePrimaryId)
                counter = 0
                allelicGeneId = ''
                for gene in returnSet:
                    counter += 1
                    allelicGeneId = gene["g.primaryKey"]
                if counter > 1:
                    allelicGeneId = ''
                    print ("returning more than one gene: this is an error")

                phenotype_features = get_phenotype_record(phenotypeRecord, dateProduced, dataProvider, release, allelicGeneId)

                list_to_yield.append(phenotype_features)
                if len(list_to_yield) == batch_size:
                    yield list_to_yield

                    list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield