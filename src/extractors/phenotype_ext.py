import uuid
from services import UrlService
from services import CreateCrossReference
from .resource_descriptor_ext import ResourceDescriptor
from loaders.transactions import Transaction

class PhenotypeExt(object):

    def get_phenotype_data(phenotype_data, batch_size, graph):
        list_to_yield = []
        xrefUrlMap = ResourceDescriptor().get_data()
        primaryId = phenotype_data.get('objectId')
        phenotypeStatement = phenotype_data.get('phenotypeStatement')
        dateProduced = phenotype_data['metaData']['dateProduced']

        for dataProviderObject in phenotype_data['metaData']['dataProvider']:

            dataProviderCrossRef = dataProviderObject.get('crossReference')
            dataProviderType = dataProviderObject.get('type')
            dataProvider = dataProviderCrossRef.get('id')
            dataProviderPages = dataProviderCrossRef.get('pages')
            dataProviderCrossRefSet = []

            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, xrefUrlMap, dataProvider, dataProviderPage)
                dataProviderCrossRefSet.append(
                    CreateCrossReference.get_xref(dataProvider, dataProvider, dataProviderPage,
                                                  dataProviderPage, dataProvider, crossRefCompleteUrl, dataProvider + dataProviderPage))

                pubMedId = phenotype_data.get('pubMedId')
                pubMedPrefix = pubMedId.split(":")[0]
                pubMedLocalId = pubMedId.split(":")[1]

                pubModId = phenotype_data.get('pubModId')
                pubModPrefix = pubModId.split(":")[0]
                pubModLocalId = pubModId.split(":")[1]

                dateAssigned = phenotype_data.get('dateAssigned')

                query = "match (g:Gene)-[:IS_ALLELE_OF]-(f:Feature) where f.primaryKey = {parameter} return g.primaryKey"
                tx = Transaction(graph)
                returnSet = tx.run_single_parameter_query(query, primaryId)
                counter = 0
                allelicGeneId = ''

                for gene in returnSet:
                    counter += 1
                    allelicGeneId = gene["g.primaryKey"]

                if counter > 1:
                    allelicGeneId = ''
                    print ("returning more than one gene: this is an error")

                elif counter < 1:
                    phenotype_feature = {
                        "primaryId": primaryId,
                        "phenotypeStatement": phenotypeStatement,
                        "dateAssigned": dateAssigned,
                        "pubMedId": pubMedId,
                        "pubMedUrl": UrlService.get_no_page_complete_url(pubMedLocalId, xrefUrlMap, pubMedPrefix,
                                                                         primaryId),
                        "pubModId": pubModId,
                        "pubModUrl": UrlService.get_page_complete_url(pubModLocalId, xrefUrlMap, pubModPrefix,
                                                                      "gene/references"),
                        "pubPrimaryKey": pubMedId + pubModId,
                        "uuid": str(uuid.uuid4()),
                        "loadKey": dataProvider + "_" + dateProduced + "_phenotype",
                        "type": "gene"
                    }

                else:

                    phenotype_feature = {
                        "primaryId": primaryId,
                        "phenotypeStatement": phenotypeStatement,
                        "dateAssigned": dateAssigned,
                        "pubMedId": pubMedId,
                        "pubMedUrl": UrlService.get_no_page_complete_url(pubMedLocalId, xrefUrlMap, pubMedPrefix,
                                                                         primaryId),
                        "pubModId": pubModId,
                        "pubModUrl": UrlService.get_page_complete_url(pubModLocalId, xrefUrlMap, pubModPrefix,
                                                                      "gene/references"),
                        "pubPrimaryKey": pubMedId + pubModId,
                        "uuid": str(uuid.uuid4()),
                        "loadKey": dataProvider + "_" + dateProduced + "_phenotype",
                        "allelicGeneId": allelicGeneId,
                        "type": "feature"
                    }

            return phenotype_feature
