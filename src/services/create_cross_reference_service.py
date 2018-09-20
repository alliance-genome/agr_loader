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
                MERGE (id:CrossReference:Identifier {primaryKey:event.primaryKey})
                    SET id.name = event.id,
                     id.globalCrossRefId = event.globalCrossRefId,
                     id.localId = event.localId,
                     id.crossRefCompleteUrl = event.crossRefCompleteUrl,
                     id.prefix = event.prefix,
                     id.crossRefType = event.crossRefType,
                     id.uuid = event.uuid,
                     id.page = event.page,
                     id.primaryKey = event.primaryKey,
                     id.displayName = event.displayName

                MERGE (o)-[gcr:CROSS_REFERENCE]->(id) """
