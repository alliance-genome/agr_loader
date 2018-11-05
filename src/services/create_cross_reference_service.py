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
