import uuid
from loaders.transactions import Transaction


class CreateCrossReference(object):

    def get_xref(self, localId, prefix, xrefUrlMap, primaryId):

        def __init__(self, localId, prefix, primaryId):
            self.localId = localId
            self.prefix = prefix
            self.primaryId = primaryId





# TODO: add bucket for panther
crossRefPrimaryId = crossRef.get('id') + '_' + primary_id
crossReferences.append({
    "id": crossRefPrimaryId,
    "globalCrossRefId": globalXrefId,
    "localId": localCrossRefId,
    "crossRefCompleteUrl": UrlService.get_no_page_complete_url(localCrossRefId, xrefUrlMap, prefix, primary_id),
    "prefix": prefix,
    "crossRefType": "gene/panther",
    "primaryKey": crossRefPrimaryId + "gene/panther",
    "uuid": str(uuid.uuid4()),
    "page": "gene/panther",
    "displayName": displayName
})