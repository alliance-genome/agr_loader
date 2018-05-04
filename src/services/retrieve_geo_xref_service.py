import uuid
from loaders.transactions import Transaction
from .create_cross_reference_service import CreateCrossReference


class RetrieveGeoXrefService(object):

    def get_geo_xref(self, global_id_list, graph):

        query = "match (g:Gene)-[crr:CROSS_REFERENCE]-(cr:CrossReference) where cr.globalCrossRefId in {parameter} return g.primaryKey, g.modLocalId, cr.name, cr.globalCrossRefId"
        geo_data = []
        tx = Transaction(graph)
        returnSet = tx.run_single_parameter_query(query, global_id_list)

        counter = 0

        for record in returnSet:
            counter += 1
            genePrimaryKey = record["g.primaryKey"]
            modLocalId = record["g.modLocalId"]
            globalCrossRefId = record["cr.globalCrossRefId"]
            geo_xref = CreateCrossReference.get_xref(globalCrossRefId.split(":")[1], "NCBI_Gene", "gene/other_expression", "gene/other_expression", "GEO", "https://www.ncbi.nlm.nih.gov/sites/entrez?Db=geoprofiles&DbFrom=gene&Cmd=Link&LinkName=gene_geoprofiles&LinkReadableName=GEO%20Profiles&IdsFromResult="+globalCrossRefId.split(":")[1], globalCrossRefId)
            geo_xref["genePrimaryKey"] = genePrimaryKey
            geo_xref["modLocalId"] = modLocalId

            geo_data.append(geo_xref)

        return geo_data



