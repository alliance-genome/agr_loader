import uuid
from loaders.transactions import Transaction


class RetrieveGeoXrefService(object):

    def get_geo_xref(self, local_id, global_id, graph):

        query = "match (g:Gene)-[crr:CROSS_REFERENCE]-(cr:CrossReference) where cr.globalCrossRefId = {parameter} return g.primaryKey, g.modLocalId, cr.name"
        geo_data = {}
        tx = Transaction(graph)
        returnSet = tx.run_single_parameter_query(query, global_id)

        counter = 0

        for record in returnSet:
            counter += 1
            genePrimaryKey = record["g.primaryKey"]
            modLocalId = record["g.modLocalId"]
            geo_data = {
                "genePrimaryKey": genePrimaryKey,
                "modLocalId": modLocalId,
                "crossRefCompleteUrl": "https://www.ncbi.nlm.nih.gov/sites/entrez?Db=geoprofiles&DbFrom=gene&Cmd=Link&LinkName=gene_geoprofiles&LinkReadableName=GEO%20Profiles&IdsFromResult="+local_id,
                "id": global_id,
                "globalCrossRefId": global_id,
                "localId": local_id,
                "prefix": "NCBI_Gene",
                "crossRefType": "gene/generic_expression",
                "primaryKey": global_id + "gene/generic_expression",
                "uuid": str(uuid.uuid4())
            }

        if counter > 1:
            genePrimaryKey = None
            modLocalId = None
            print ("returning more than one gene: this is an error")

        return geo_data

