import uuid
from services import UrlService
from services import CreateCrossReference
from .resource_descriptor_ext import ResourceDescriptor
from loaders.transactions import Transaction

def get_phenotype_record(phenotypeRecord, dataProvider, dateProduced, release, graph):
    xrefUrlMap = ResourceDescriptor().get_data()
    primaryId = phenotypeRecord.get('objectId')
    phenotypeStatement = phenotypeRecord.get('phenotypeStatement')

    pubMedId = phenotypeRecord.get('pubMedId')
    pubMedPrefix = pubMedId.split(":")[0]
    pubMedLocalId = pubMedId.split(":")[1]

    pubModId = phenotypeRecord.get('pubModId')
    pubModPrefix = pubModId.split(":")[0]
    pubModLocalId = pubModId.split(":")[1]

    dateAssigned = phenotypeRecord.get('dateAssigned')

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


    phenotype_feature = {
            "primaryId": primaryId,
            "phenotypeStatement": phenotypeStatement,
            "dateAssigned": dateAssigned,
            "pubMedId": pubMedId,
            "pubMedUrl": UrlService.get_no_page_complete_url(pubMedLocalId, xrefUrlMap, pubMedPrefix, primaryId),
            "pubModId": pubModId,
            "pubModUrl": UrlService.get_page_complete_url(pubModLocalId, xrefUrlMap, pubModPrefix, "gene/references"),
            "pubPrimaryKey": pubMedId + pubModId,
            "uuid": str(uuid.uuid4()),
            "loadKey": dataProvider + "_" + dateProduced + "_phenotype",
            "allelicGeneId": allelicGeneId
    }
    return phenotype_feature
