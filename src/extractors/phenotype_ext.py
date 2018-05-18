import uuid


def get_phenotype_record(phenotypeRecord, dataProvider, dateProduced, release, allelicGeneId):

            fishEnvId = None
            conditions = None
            qualifier = None
            publicationModId = None
            pubMedId = None

            phenotypeObjectType = phenotypeRecord['objectRelation'].get("objectType")

            primaryId = phenotypeRecord.get('objectId')

            if 'qualifier' in phenotypeRecord:
                qualifier = phenotypeRecord.get('qualifier')
            if qualifier is None:
                if 'evidence' in phenotypeRecord:

                    publicationModId = ""
                    pubMedId = ""
                    pubModUrl = None
                    pubMedUrl = None
                    phenotypeAssociationType = None
                    ecodes = []

                    evidence = phenotypeRecord.get('evidence')
                    if 'publication' in evidence:
                        if 'modPublicationId' in evidence['publication']:
                            publicationModId = evidence['publication'].get('modPublicationId')
                            localPubModId = publicationModId.split(":")[1]
                            pubModUrl = get_complete_pub_url(localPubModId, publicationModId)
                        if 'pubMedId' in evidence['publication']:
                            pubMedId = evidence['publication'].get('pubMedId')
                            localPubMedId = pubMedId.split(":")[1]
                            pubMedUrl = get_complete_pub_url(localPubMedId, pubMedId)

                if 'objectRelation' in phenotypeRecord:
                    phenotypeAssociationType = phenotypeRecord['objectRelation'].get("associationType")

                    additionalGeneticComponents = []
                    if 'additionalGeneticComponents' in phenotypeRecord['objectRelation']:
                        for component in phenotypeRecord['objectRelation']['additionalGeneticComponents']:
                            componentSymbol = component.get('componentSymbol')
                            componentId = component.get('componentId')
                            componentUrl = component.get('componentUrl') + componentId
                            additionalGeneticComponents.append(
                                {"id": componentId, "componentUrl": componentUrl, "componentSymbol": componentSymbol}
                            )

                if 'evidenceCodes' in phenotypeRecord['evidence']:
                    ecodes = phenotypeRecord['evidence'].get('evidenceCodes')

                if 'experimentalConditions' in phenotypeRecord:
                    conditionId = ""
                    for condition in phenotypeRecord['experimentalConditions']:
                        if 'textCondition' in condition:
                            if dataProvider == 'ZFIN':
                                conditionId = conditionId + condition.get('textCondition')
                                # if condition != None:
                    conditions = phenotypeRecord.get('experimentalConditions')
                if dataProvider == 'ZFIN':
                    fishEnvId = primaryId + conditionId

                # TODO: get SGD to fix their phenotype file.
                if phenotypeRecord.get('taxonId') == 'taxon:559292':
                    taxonId = "NCBITaxon:559292"
                else:
                    taxonId = phenotypeRecord.get('taxonId')

                phenotype_feature = {
                    "primaryId": primaryId,
                    "phenotypeObjectName": phenotypeRecord.get('objectName'),
                    "phenotypeObjectType": phenotypeObjectType,
                    "taxonId": taxonId,
                    "phenotypeAssociationType": phenotypeRecord['objectRelation'].get("associationType"),
                    "with": phenotypeRecord.get('with'),
                    "doId": phenotypeRecord.get('DOid'),
                    "pubMedId": pubMedId,
                    "pubMedUrl": pubMedUrl,
                    "pubModId": publicationModId,
                    "pubModUrl": pubModUrl,
                    "pubPrimaryKey": pubMedId + publicationModId,
                    "release": release,
                    "dataProvider": dataProvider,
                    "relationshipType": phenotypeAssociationType,
                    "dateProduced": dateProduced,
                    "qualifier": qualifier,
                    "doDisplayId": phenotypeRecord.get('DOid'),
                    "doUrl": "http://www.phenotype-ontology.org/?id=" + phenotypeRecord.get('DOid'),
                    "doPrefix": "DOID",
                    # doing the typing in neo, but this is for backwards compatibility in ES
                    "ecodes": ecodes,
                    "definition": phenotypeRecord.get('definition'),
                    "inferredGene": phenotypeRecord.get('objectRelation').get('inferredGeneAssociation'),
                    "experimentalConditions": conditions,
                    "fishEnvId": fishEnvId,
                    "additionalGeneticComponents": additionalGeneticComponents,
                    "uuid": str(uuid.uuid4()),
                    "loadKey": dataProvider + "_" + dateProduced + "_phenotype",
                    "allelicGeneId": allelicGeneId
                }
                return phenotype_feature


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
