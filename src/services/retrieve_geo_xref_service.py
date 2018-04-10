import uuid
from loaders.transactions import Transaction


class RetrieveGeoXrefService(object):

    def get_geo_xref(self, local_id, global_id, graph):

        query = "match (g:Gene)-[crr:CROSS_REFERENCE]-(cr:CrossReference) where cr.globalCrossRefId = {parameter} return cr, g"
        geo_data = {}
        tx = Transaction(graph)
        parameter = global_id
        returnSet = tx.run_single_parameter_query(query, parameter)
        counter = 0

        for x in returnSet:
            print (x[0])
            counter += 1
            genePrimaryKey = x["g.primaryKey"]
            modLocalId = x["g.modLocalId"]
            print ("here is the return value for genePrimaryKey" + genePrimaryKey)
            geo_data = {
                "genePrimaryKey": genePrimaryKey,
                "modLocalId": modLocalId,
                "crossRefCompleteUrl": "https://www.ncbi.nlm.nih.gov/sites/entrez?Db=geoprofiles&DbFrom=gene&Cmd=Link&LinkName=gene_geoprofiles&LinkReadableName=GEO%20Profiles&IdsFromResult="+local_id,
                "id": global_id,
                "globalCrossRefId": global_id,
                "localId": local_id,
                "prefix": "NCBI_Gene",
                "crossRefType": "gene/geo",
                "primaryKey": global_id + "gene/geo",
                "uuid": str(uuid.uuid4())
            }


        if counter > 1:
            genePrimaryKey = None
            modLocalId = None
            print ("returning more than one gene: this is an error")

        return geo_data

