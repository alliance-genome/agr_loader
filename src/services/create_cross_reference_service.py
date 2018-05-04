import uuid


class CreateCrossReference(object):

    def get_xref(localId, prefix, crossRefType, page, displayName, crossRefCompleteUrl, globalXrefId):
        crossReference = {
            "id": globalXrefId,
            "globalCrossRefId": globalXrefId,
            "localId": localId,
            "prefix": prefix,
            "crossRefType": crossRefType,
            "primaryKey": globalXrefId,
            "uuid":  str(uuid.uuid4()),
            "page": page,
            "displayName": displayName,
            "crossRefCompleteUrl": crossRefCompleteUrl,
            "name": globalXrefId
        }
        return crossReference
