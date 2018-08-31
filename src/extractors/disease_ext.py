import uuid
from services import SpeciesService
from services import UrlService
from services import CreateCrossReference
from .resource_descriptor_ext import ResourceDescriptor
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def get_disease_record(diseaseRecord, dataProviders, dateProduced, release, allelicGeneId, dataProviderSingle):
    fishEnvId = None
    conditions = None
    qualifier = None
    publicationModId = None
    pubMedId = None

    diseaseObjectType = diseaseRecord['objectRelation'].get("objectType")

    primaryId = diseaseRecord.get('objectId')

    loadKey = dateProduced + "_Disease"
    annotationUuid = str(uuid.uuid4())

    for dataProvider in dataProviders:
        loadKey = dataProvider + loadKey

    if 'qualifier' in diseaseRecord:
        qualifier = diseaseRecord.get('qualifier')

    if qualifier is None:
        if 'evidence' in diseaseRecord:

            publicationModId = ""
            pubMedId = ""
            pubModUrl = None
            pubMedUrl = None
            diseaseAssociationType = None
            ecodes = []

            evidence = diseaseRecord.get('evidence')
            if 'publication' in evidence:
                if 'modPublicationId' in evidence['publication']:
                    publicationModId = evidence['publication'].get('modPublicationId')
                    localPubModId = publicationModId.split(":")[1]
                    pubModUrl = get_complete_pub_url(localPubModId, publicationModId)
                if 'pubMedId' in evidence['publication']:
                    pubMedId = evidence['publication'].get('pubMedId')
                    localPubMedId = pubMedId.split(":")[1]
                    pubMedUrl = get_complete_pub_url(localPubMedId, pubMedId)

        if 'objectRelation' in diseaseRecord:
            diseaseAssociationType = diseaseRecord['objectRelation'].get("associationType")

            additionalGeneticComponents = []
            if 'additionalGeneticComponents' in diseaseRecord['objectRelation']:
                for component in diseaseRecord['objectRelation']['additionalGeneticComponents']:
                    componentSymbol = component.get('componentSymbol')
                    componentId = component.get('componentId')
                    componentUrl = component.get('componentUrl') + componentId
                    additionalGeneticComponents.append(
                        {"id": componentId, "componentUrl": componentUrl, "componentSymbol": componentSymbol}
                    )
        annotationDataProviders ={}

        if 'dataProvider' in diseaseRecord:
            for dp in diseaseRecord['dataProvider']:
                annotationType = dp.get('type')
                dpCrossRefId = dp['crossReference'].get('id')
                dpPages = dp['crossReference'].get('pages')
                annotationDataProviders[uuid] = {"annoationType": annotationType,
                                                 "dpCrossRefId": dpCrossRefId,
                                                 "dpPages": dpPages}
        if 'evidenceCodes' in diseaseRecord['evidence']:
            ecodes = diseaseRecord['evidence'].get('evidenceCodes')

        if diseaseRecord.get('taxonId') == 'taxon:559292':
            taxonId = "NCBITaxon:559292"
        else:
            taxonId = diseaseRecord.get('taxonId')

        disease_feature = {
            "primaryId": primaryId,
            "diseaseObjectName": diseaseRecord.get('objectName'),
            "diseaseObjectType": diseaseObjectType,
            "taxonId": taxonId,
            "diseaseAssociationType": diseaseRecord['objectRelation'].get("associationType"),
            "with": diseaseRecord.get('with'),
            "doId": diseaseRecord.get('DOid'),
            "pubMedId": pubMedId,
            "pubMedUrl": pubMedUrl,
            "pubModId": publicationModId,
            "pubModUrl": pubModUrl,
            "pubPrimaryKey": pubMedId + publicationModId,
            "release": release,
            "dataProviders": dataProviders,
            "dataProvider": dataProviderSingle,
            "relationshipType": diseaseAssociationType,
            "dateProduced": dateProduced,
            "qualifier": qualifier,
            "doDisplayId": diseaseRecord.get('DOid'),
            "doUrl": "http://www.disease-ontology.org/?id=" + diseaseRecord.get('DOid'),
            "doPrefix": "DOID",
            # doing the typing in neo, but this is for backwards compatibility in ES
            "ecodes": ecodes,
            "definition": diseaseRecord.get('definition'),
            "inferredGene": diseaseRecord.get('objectRelation').get('inferredGeneAssociation'),
            "experimentalConditions": conditions,
            "fishEnvId": fishEnvId,
            "additionalGeneticComponents": additionalGeneticComponents,
            "uuid": annotationUuid,
            "loadKey": loadKey,
            "allelicGeneId": allelicGeneId
        }
        return disease_feature


def get_complete_pub_url(local_id, global_id):
    complete_url = None

    if 'MGI' in global_id:
        complete_url = 'http://www.informatics.jax.org/accession/' + global_id
    if 'RGD' in global_id:
        complete_url = 'http://rgd.mcw.edu/rgdweb/search/search.html?term=' + local_id
    if 'SGD' in global_id:
        complete_url = 'http://www.yeastgenome.org/reference/' + local_id
    if 'FB' in global_id:
        complete_url = 'http://flybase.org/reports/' + local_id + '.html'
    if 'ZFIN' in global_id:
        complete_url = 'http://zfin.org/' + local_id
    if 'WB:' in global_id:
        complete_url = 'http://www.wormbase.org/db/misc/paper?name=' + local_id
    if 'PMID:' in global_id:
        complete_url = 'https://www.ncbi.nlm.nih.gov/pubmed/' + local_id

    return complete_url
