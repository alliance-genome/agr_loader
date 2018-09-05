import uuid
from loaders.transactions.transaction import Transaction
#from .create_cross_reference_service import CreateCrossReference

class RetrieveDiseaseByOrthologyService(object):

    def retreive_orthologous_diseases_gene(self, global_id_list, graph):

        query = """
MATCH (disease:DOTerm)-[da]-(gene1:Gene)-[o:ORTHOLOGOUS]-(gene2:Gene)
MATCH (dej:DiseaseEntityJoin)-[e]-(gene1)-[FROM_SPECIES]->(species:Species)
    WHERE da.uuid = dej.primaryKey
      AND o.strictFilter
OPTIONAL MATCH (gene2:Gene)-[da2]-(disease:DOTerm)
    WHERE da2 IS null
RETURN gene2.primaryKey AS geneID,
       species.primaryKey AS speciesID,
       disease.primaryKey AS DOID"""
        orthologous_disease_data = []
        tx = Transaction(graph)
        returnSet = tx.run_single_query(query)
        print(returnSet)
        exit()
        counter = 0

        for record in returnSet:
            print(record)
#            counter += 1
#            genePrimaryKey = record["g.primaryKey"]
#            modLocalId = record["g.modLocalId"]
#            globalCrossRefId = record["cr.globalCrossRefId"]
#            geo_xref = CreateCrossReference.get_xref(globalCrossRefId.split(":")[1], "NCBI_Gene", "gene/other_expression", "gene/other_expression", "GEO", "https://www.ncbi.nlm.nih.gov/sites/entrez?Db=geoprofiles&DbFrom=gene&Cmd=Link&LinkName=gene_geoprofiles&LinkReadableName=GEO%20Profiles&IdsFromResult="+globalCrossRefId.split(":")[1], globalCrossRefId+"gene/other_expression")
#            geo_xref["genePrimaryKey"] = genePrimaryKey
#            geo_xref["modLocalId"] = modLocalId
#
#            geo_data.append(geo_xref)
#
        return orthologous_disease_data



