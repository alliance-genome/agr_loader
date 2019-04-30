import uuid
import logging

from etl.helpers import ETLHelper

logger = logging.getLogger(__name__)


class DiseaseHelper(object):

    @staticmethod
    def get_disease_record(diseaseRecord, dataProviders, dateProduced, release, allelicGeneId, dataProviderSingle):
        qualifier = None
        publicationModId = None
        pubMedId = None

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
                if 'publicationId' in evidence:
                    if evidence.get('publicationId').startswith('PMID:'):
                        pubMedId = evidence['publicationId']
                        localPubMedId = pubMedId.split(":")[1]
                        pubMedUrl = ETLHelper.get_complete_pub_url(localPubMedId, pubMedId)
                        if 'crossReference' in evidence:
                            pubXref = evidence.get('crossReference')
                            publicationModId = pubXref.get('id')
                            localPubModId = publicationModId.split(":")[1]
                            pubModUrl = ETLHelper.get_complete_pub_url(localPubModId, publicationModId)
                    else:
                        publicationModId = evidence['publicationId']
                        localPubModId = publicationModId.split(":")[1]
                        pubModUrl = ETLHelper.get_complete_pub_url(localPubModId, publicationModId)

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
            annotationDataProviders = {}

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

            doId = diseaseRecord.get('DOid')
            diseaseUniqueKey = primaryId+doId+diseaseAssociationType

            disease_allele = {
                "diseaseUniqueKey": diseaseUniqueKey,
                "doId": doId,
                "primaryId": primaryId,
                "uuid": annotationUuid,
                "dataProviders": dataProviders,
                "relationshipType": diseaseAssociationType,
                "dateProduced": dateProduced,
                "dataProvider": dataProviderSingle,
                
                "pubPrimaryKey": pubMedId + publicationModId,
                
                "pubModId": publicationModId,
                "pubMedId": pubMedId,
                "pubMedUrl": pubMedUrl,
                "pubModUrl": pubModUrl,
                
                "ecodes": ecodes,

            }
            return disease_allele
