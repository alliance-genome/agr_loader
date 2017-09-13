import uuid

class DiseaseExt(object):


    def get_features(self, disease_data, batch_size, testObject):
        disease_features = {}
        list_to_yield = []
        dateProduced = disease_data['metaData']['dateProduced']
        dataProvider = disease_data['metaData']['dataProvider']
        release = None

        if 'release' in disease_data['metaData']:
            release = disease_data['metaData']['release']

        for diseaseRecord in disease_data['data']:
            fishEnvId = None
            conditions = None
            qualifier = None
            publicationModId = None
            pubMedId = None

            # Only processing genes for 1.0 
            diseaseObjectType = diseaseRecord['objectRelation'].get("objectType")
            if diseaseObjectType != 'gene':
                continue

            primaryId = diseaseRecord.get('objectId')

            if testObject.using_test_data() is True:
                is_it_test_entry = testObject.check_for_test_id_entry(primaryId)
                if is_it_test_entry is False:
                    continue

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
                            pubModUrl = self.get_complete_pub_url(localPubModId, publicationModId)
                        if 'pubMedId' in evidence['publication']:
                            pubMedId = evidence['publication'].get('pubMedId')
                            localPubMedId = pubMedId.split(":")[1]
                            pubMedUrl = self.get_complete_pub_url(localPubMedId, pubMedId)

                if 'objectRelation' in diseaseRecord:
                    diseaseAssociationType = diseaseRecord['objectRelation'].get("associationType")

                    additionalGeneticComponents = []
                    if 'additionalGeneticComponents' in diseaseRecord['objectRelation']:
                        for component in diseaseRecord['objectRelation']['additionalGeneticComponents']:
                            componentSymbol = component.get('componentSymbol')
                            componentId = component.get('componentId')
                            componentUrl = component.get('componentUrl')+componentId
                            additionalGeneticComponents.append(
                                {"id": componentId, "componentUrl": componentUrl, "componentSymbol": componentSymbol}
                            )

                if 'evidenceCodes' in diseaseRecord['evidence']:
                    ecodes = diseaseRecord['evidence'].get('evidenceCodes')

                if 'experimentalConditions' in diseaseRecord:
                    conditionId = ""
                    for condition in diseaseRecord['experimentalConditions']:
                        if 'textCondition' in condition:
                            if dataProvider == 'ZFIN':
                                conditionId = conditionId + condition.get('textCondition')
                        #if condition != None:
                    conditions = diseaseRecord.get('experimentalConditions')
                if dataProvider == 'ZFIN':
                    fishEnvId = primaryId+conditionId

                disease_features = {
                            "primaryId": primaryId,
                            "diseaseObjectName": diseaseRecord.get('objectName'),
                            "diseaseObjectType": diseaseObjectType,
                            "taxonId": diseaseRecord.get('taxonId'),
                            "diseaseAssociationType": diseaseRecord['objectRelation'].get("associationType"),
                            "with": diseaseRecord.get('with'),
                            "doId": diseaseRecord.get('DOid'),
                            "pubMedId": pubMedId,
                            "pubMedUrl": pubMedUrl,
                            "pubModId": publicationModId,
                            "pubModUrl": pubModUrl,
                            "pubPrimaryKey": pubMedId+publicationModId,
                            "release": release,
                            "dataProvider": dataProvider,
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
                            "additionalGeneticComponents":additionalGeneticComponents,
                            "uuid":str(uuid.uuid1())
                        }

            list_to_yield.append(disease_features)
            if len(list_to_yield) == batch_size:
                yield list_to_yield

                list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield


    def get_complete_pub_url(self, local_id, global_id):

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
