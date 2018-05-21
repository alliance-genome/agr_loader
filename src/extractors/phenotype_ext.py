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
        pubMedUrl = None
        pubMedId = None
        pubModId = None
        pubModUrl = None

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

                for pheno in phenotype_data:
                    primaryId = pheno.get('objectId')
                    phenotypeStatement = pheno.get('phenotypeStatement')

                    pubMedId = phenotype_data.get('pubMedId')
                    if pubMedId != None:
                        pubMedPrefix = pubMedId.split(":")[0]
                        pubMedLocalId = pubMedId.split(":")[1]
                        pubMedUrl = UrlService.get_no_page_complete_url(pubMedLocalId, xrefUrlMap, pubMedPrefix, primaryId)

                        pubModId = phenotype_data.get('pubModId')
                    if pubModId != None:
                        pubModPrefix = pubModId.split(":")[0]
                        pubModLocalId = pubModId.split(":")[1]
                        pubModUrl = UrlService.get_page_complete_url(pubModLocalId, xrefUrlMap, pubModPrefix, "gene/references")

                    dateAssigned = phenotype_data.get('dateAssigned')

                    if pubModId == None and pubMedId == None:
                        print (primaryId + "is missing pubMed and pubMod id")

                    query = "match (g:Gene)-[:IS_ALLELE_OF]-(f:Feature) where f.primaryKey = {parameter} return g.primaryKey"
                    tx = Transaction(graph)
                    returnSet = tx.run_single_parameter_query(query, primaryId)
                    counter = 0
                    allelicGeneId = ''

                    for gene in returnSet:
                        counter += 1
                        allelicGeneId = gene["g.primaryKey"]

                    if counter > 1:
                        print ("returning more than one gene: this is an error")

                    elif counter < 1:
                        phenotype_feature = {
                            "primaryId": primaryId,
                            "phenotypeStatement": phenotypeStatement,
                            "dateAssigned": dateAssigned,
                            "pubMedId": pubMedId,
                            "pubMedUrl": pubMedUrl,
                            "pubModId": pubModId,
                            "pubModUrl": pubModUrl,
                            "pubPrimaryKey": pubMedId + pubModId,
                            "uuid": str(uuid.uuid4()),
                            "loadKey": dataProvider + "_" + dateProduced + "_phenotype",
                            "type": "gene",
                            "dataProviderType": dataProviderType
                        }

                    else:

                        phenotype_feature = {
                            "primaryId": primaryId,
                            "phenotypeStatement": phenotypeStatement,
                            "dateAssigned": dateAssigned,
                            "pubMedId": pubMedId,
                            "pubMedUrl": pubMedUrl,
                            "pubModId": pubModId,
                            "pubModUrl": pubModUrl,
                            "pubPrimaryKey": pubMedId + pubModId,
                            "uuid": str(uuid.uuid4()),
                            "loadKey": dataProvider + "_" + dateProduced + "_phenotype",
                            "allelicGeneId": allelicGeneId,
                            "type": "feature",
                            "dataProviderType": dataProviderType
                        }

                list_to_yield.append(phenotype_feature)
                if len(list_to_yield) == batch_size:
                        yield list_to_yield
                        list_to_yield[:] = []  # Empty the list.

            if len(list_to_yield) > 0:
                yield list_to_yield