import uuid


class CreateCrossReference(object):

    def get_xref(self, localId, prefix, crossRefType, page, displayName, crossRefCompleteUrl):
        crossRefPrimaryId = prefix+":"+localId
        crossReference = {
            "id": crossRefPrimaryId,
            "globalCrossRefId": crossRefPrimaryId,
            "localId": localId,
            "prefix": prefix,
            "crossRefType": crossRefType,
            "primaryKey": crossRefPrimaryId,
            "uuid":  str(uuid.uuid4()),
            "page": page,
            "displayNae": displayName,
            "crossRefCompleteUrl": crossRefCompleteUrl
        }
        return crossReference
