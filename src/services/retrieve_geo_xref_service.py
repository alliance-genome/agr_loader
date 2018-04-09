import uuid
from loaders.transactions import Transaction


class RetrieveGeoXrefService(object):

    def get_geo_xref(self, local_id, global_id, graph):

        query = "match (g:Gene)-[]-(cr:CrossReference) where cr.globalCrossRefId = {parameter} return g.primaryKey, g.modLocalId"
        pk = global_id
        geo_data = {}
        #print ("here is the global_id:"+ global_id)
        tx = Transaction(graph)
        returnSet = tx.run_single_parameter_query(query, pk)
        print (returnSet)
        counter = 0
        for gene in returnSet:
            counter += 1
            genePrimaryKey = gene['g.primaryKey']
            modLocalId = gene['g.modLocalId']
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

