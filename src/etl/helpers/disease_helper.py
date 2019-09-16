import uuid
import logging

from . import ETLHelper

logger = logging.getLogger(__name__)


class DiseaseHelper(object):

    @staticmethod
    def get_disease_record(diseaseRecord, dataProviders, dateProduced, release, allelicGeneId, dataProviderSingle):
        qualifier = None
        publicationModId = None
        pubMedId = None
        annotationDP = []
        pgeKey = ''

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
                annotationDataProvider = {}
                annotationDP = []

                evidence = diseaseRecord.get('evidence')
                if 'publication' in evidence:
                    publication = evidence.get('publication')
                    if publication.get('publicationId').startswith('PMID:'):
                        pubMedId = publication.get('publicationId')
                        localPubMedId = pubMedId.split(":")[1]
                        pubMedUrl = ETLHelper.get_complete_pub_url(localPubMedId, pubMedId)
                        if 'crossReference' in evidence:
                            pubXref = evidence.get('crossReference')
                            publicationModId = pubXref.get('id')
                            localPubModId = publicationModId.split(":")[1]
                            pubModUrl = ETLHelper.get_complete_pub_url(localPubModId, publicationModId)
                    else:
                        publicationModId = publication.get('publicationId')
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

            if 'dataProvider' in diseaseRecord:
                for dp in diseaseRecord['dataProvider']:

                    annotationType = dp.get('type')
                    xref = dp.get('crossReference')
                    crossRefId = xref.get('id')
                    pages = xref.get('pages')

                    annotationDataProvider = {"annotationType": annotationType,
                                                     "crossRefId": crossRefId,
                                                     "dpPages": pages}
                    annotationDP.append(annotationDataProvider)
            if 'evidenceCodes' in diseaseRecord['evidence']:
                ecodes = diseaseRecord['evidence'].get('evidenceCodes')

            doId = diseaseRecord.get('DOid')
            diseaseUniqueKey = primaryId+doId+diseaseAssociationType

            if 'primaryGeneticEntityIDs' in diseaseRecord:
                pgeIds = diseaseRecord.get('primaryGeneticEntityIDs')
                for pge in pgeIds:
                    pgeKey = pgeKey+pge

            else:
                pgeIds = []

            pecjPrimaryKey = publicationModId + pubMedId + pgeKey

            disease_allele = {
                "diseaseUniqueKey": diseaseUniqueKey,
                "doId": doId,
                "primaryId": primaryId,
                "uuid": annotationUuid,
                "dataProviders": dataProviders,
                "relationshipType": diseaseAssociationType,
                "dateProduced": dateProduced,
                "dataProvider": dataProviderSingle,
                "dateAssigned": diseaseRecord["dateAssigned"],
                
                "pubPrimaryKey": pecjPrimaryKey,
                
                "pubModId": publicationModId,
                "pubMedId": pubMedId,
                "pubMedUrl": pubMedUrl,
                "pubModUrl": pubModUrl,
                "pgeIds": pgeIds,
                "pgeKey": pgeKey,
                "annotationDP": annotationDP,
                "ecodes": ecodes,

            }
            return disease_allele
