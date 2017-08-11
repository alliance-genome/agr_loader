from files import *
import re
from .test_check import check_for_test_entry

class DiseaseExt:


    def get_features(self, disease_data, batch_size, test_set):
        disease_features = {}
        list_to_yield = []
        qualifier = None;

        dateProduced = disease_data['metaData']['dateProduced']
        dataProvider = disease_data['metaData']['dataProvider']
        release = None
        publicationModId = None
        pubMedId = None
        fishEnvId = None
        conditions = None
        if 'release' in disease_data['metaData']:
            release = disease_data['metaData']['release']

        for diseaseRecord in disease_data['data']:
            primaryId = diseaseRecord.get('objectId')
            if test_set == True:
                is_it_test_entry = check_for_test_entry(primaryId)
                if is_it_test_entry == False:
                    continue

            if 'qualifier' in diseaseRecord:
                qualifier = diseaseRecord.get('qualifier')
            if qualifier is None:


                if 'evidence' in diseaseRecord:
                    # this is purposeful for the moment, need to concantenate two strings, both of which has the possibility of being null -- so setting as an empty string
                    # instead of none.
                    publicationModId = ""
                    pubMedId = ""
                    pubModUrl = None
                    pubMedUrl = None
                    evidence = diseaseRecord.get('evidence')
                    if 'publication' in evidence:
                        if 'modPublicationId' in evidence['publication']:
                            publicationModId = evidence['publication'].get('modPublicationId')
                            localPubModId = publicationModId.split(":")[1]
                            pubModUrl = self.get_complete_pub_url(localPubModId, publicationModId)
                        if 'pubMedId' in evidence['publication']:
                            pubMedId = evidence['publication'].get('pubMedId')
                            localPubMedId = publicationModId.split(":")[1]
                            pubMedUrl = self.get_complete_pub_url(localPubMedId, pubMedId)


                if 'objectRelation' in diseaseRecord:
                    diseaseObjectType = diseaseRecord['objectRelation'].get("objectType")
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
                    #print (diseaseRecord['evidence']['evidenceCodes'])
                    ecodes = diseaseRecord['evidence'].get('evidenceCodes')

                if 'experimentalConditions' in diseaseRecord:
                    conditionId = None

                    for condition in diseaseRecord['experimentalConditions']:
                        if 'textCondition' in condition:
                            if dataProvider == 'ZFIN':
                                conditionId = conditionId + condition.get('textCondition')
                        conditions.append(condition)
                if dataProvider == 'ZFIN':
                    fishEnvId = primaryId+conditionId

                diseaseObjectType = diseaseRecord['objectRelation'].get("objectType")
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
                            "dateProduced":dateProduced,
                            #note: for now we will never get this, because we're suppressing NOT qualifiers for 1.0 release TODO: let these back in -- relationships
                            #are already handled in the disease.py, cypher query tx.
                            "qualifier": qualifier,
                            "doDisplayId": diseaseRecord.get('DOid'),
                            "doUrl": "http://www.disease-ontology.org/?id=" + diseaseRecord.get('DOid'),
                            "doPrefix": "DOID",
                            # doing the typing in neo, but this is for backwards compatibility in ES
                            "diseaseObjectType": diseaseRecord.get('objectRelation').get('objectType'),
                            "diseaseAssociationId": primaryId+diseaseRecord.get('DOid'),
                            "diseaseAssociationPubId": primaryId+diseaseRecord.get('DOid')+pubMedId+publicationModId,
                            "ecodes": ecodes,
                            "inferredGene": diseaseRecord.get('objectRelation').get('inferredGeneAssociation'),
                            "experimentalConditions": conditions,
                            "fishEnvId": fishEnvId
                        }
                qualifier = None
                fishEnvId = None

               # print (disease_features)
            list_to_yield.append(disease_features)
            if len(list_to_yield) == batch_size:
                #print (list_to_yield)
                yield list_to_yield

                list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            #print (list_to_yield)
            yield list_to_yield

    def get_data(self, disease_data):

        disease_annots = {}
        list_to_yield = []

        dateProduced = disease_data['metaData']['dateProduced']
        dataProvider = disease_data['metaData']['dataProvider']
        release = None

        if 'release' in disease_data['metaData']:
            release = disease_data['metaData']['release']

        for diseaseRecord in disease_data['data']:


            if 'experimentalConditions' in diseaseRecord:
                for experimentalCondition in diseaseRecord['experimentalConditions']:
                    experimentalConditions.append({"zecoId": experimentalCondition.get('zecoId'),
                                                   "geneOntologyId": experimentalCondition.get('geneOntologyId'),
                                                   "ncbiTaxonId": experimentalCondition.get('ncbiTaxonID'),
                                                   "chebiOntologyId": experimentalCondition.get('chebiOntologyId'),
                                                   "anatomicalId": experimentalCondition.get('anatomicalId'),
                                                   "experimentalConditionIsStandard": experimentalCondition.get(
                                                       'conditiionIsStandard'),
                                                   "freeTextCondition": experimentalCondition.get('textCondition')})


            if modifierQualifier is None and qualifier is None:
                disease_annots[primaryId].append({
                    "diseaseObjectName": diseaseRecord.get('objectName'),
                    "qualifier": diseaseRecord.get('qualifier'),
                    "with": diseaseRecord.get('with'),
                    "taxonId": diseaseRecord.get('taxonId'),
                    "geneticSex": diseaseRecord.get('geneticSex'),
                    "dataAssigned": diseaseRecord.get('dateAssigned'),
                    "experimentalConditions": experimentalConditions,
                    "associationType": diseaseRecord.get('objectRelation').get('associationType'),
                    "diseaseObjectType": diseaseRecord.get('objectRelation').get('objectType'),
                    #"evidenceList": evidenceList,
                    "modifier": modifier,
                    #"objectRelation": objectRelationMap,
                    "evidence": evidenceList,
                    "do_id": diseaseRecord.get('DOid'),
                    "do_name": None,
                    "dateProduced": dateProduced,
                    "release": release,
                    "dataProvider": dataProvider,
                    "doIdDisplay": {"displayId": diseaseRecord.get('DOid'), "url": "http://www.disease-ontology.org/?id=" + diseaseRecord.get('DOid'), "prefix": "DOID"}
                })
        return disease_annots

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
        if 'PUBMED:' in global_id:
            complete_url = 'https://www.ncbi.nlm.nih.gov/pubmed/' + local_id

        return complete_url
