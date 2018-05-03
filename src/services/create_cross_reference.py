import uuid


class CreateCrossReference(object):

    def get_xref(self, localId, prefix, primaryId, crossRefType, page, displayName):
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
            "displayNae": displayName
        }
        return crossReference
