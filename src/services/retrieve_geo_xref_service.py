import uuid
from loaders.transactions import Transaction


class RetrieveGeoXrefService(object):

    def get_geo_xref(self, global_id_list, graph):

        query = "match (g:Gene)-[crr:CROSS_REFERENCE]-(cr:CrossReference) where cr.globalCrossRefId in {parameter} return g.primaryKey, g.modLocalId, cr.name, cr.globalCrossRefId"
        geo_data = {}
        tx = Transaction(graph)
        returnSet = tx.run_single_parameter_query(query, global_id_list)

        counter = 0

        for record in returnSet:
            counter += 1
            genePrimaryKey = record["g.primaryKey"]
            modLocalId = record["g.modLocalId"]
            globalCrossRefId = record["cr.globalCrossRefId"]
            geo_data = {
                "genePrimaryKey": genePrimaryKey,
                "modLocalId": modLocalId,
                "crossRefCompleteUrl": "https://www.ncbi.nlm.nih.gov/sites/entrez?Db=geoprofiles&DbFrom=gene&Cmd=Link&LinkName=gene_geoprofiles&LinkReadableName=GEO%20Profiles&IdsFromResult="+local_id,
                "id": globalCrossRefId,
                "globalCrossRefId": globalCrossRefId,
                "localId": globalCrossRefId.split(":")[1],
                "prefix": "NCBI_Gene",
                "crossRefType": "gene/other_expression",
                "primaryKey": globalCrossRefId + "gene/other_expression",
                "uuid": str(uuid.uuid4())
            }

        return geo_data

