import uuid


class CreateCrossReference(object):

    def get_xref(localId, prefix, crossRefType, page, displayName, crossRefCompleteUrl, primaryId):
        globalXrefId = prefix+":"+localId
        crossReference = {
            "id": globalXrefId,
            "globalCrossRefId": globalXrefId,
            "localId": localId,
            "prefix": prefix,
            "crossRefType": crossRefType,
            "primaryKey": primaryId,
            "uuid":  str(uuid.uuid4()),
            "page": page,
            "displayName": displayName,
            "crossRefCompleteUrl": crossRefCompleteUrl,
            "name": globalXrefId
        }
        return crossReference

    # make one place to create a cross reference node with consistent attributes
    def get_cypher_xref_text(objectType):

        return """
                MERGE (id:CrossReference {primaryKey:event.primaryKey})
                    SET id.name = event.id
                    SET id.globalCrossRefId = event.globalCrossRefId
                    SET id.localId = event.localId
                    SET id.crossRefCompleteUrl = event.crossRefCompleteUrl
                    SET id.prefix = event.prefix
                    SET id.crossRefType = event.crossRefType
                    SET id.uuid = event.uuid
                    SET id.page = event.page
                    SET id.primaryKey = event.primaryKey
                    SET id.displayName = event.displayName

                MERGE (o)-[gcr:CROSS_REFERENCE]->(id) """
