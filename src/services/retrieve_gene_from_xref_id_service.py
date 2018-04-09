from loaders.transactions import Transaction


class RetrieveGeneFromXrefIdService(object):

    def get_gene_from_id(local_id, global_id, graph):
        # Local and global are cross references, primary is the gene id.
        complete_url = None

        query = "match (g:Gene)-[]-(cr:CrossReference) where cr.globalCrossRefId = {parameter} return g.primaryKey, g.modLocalId"
        pk = global_id
        modLocalId = ""
        page_url_prefix = ""
        page_url_suffix = ""

        tx = Transaction(graph)
        returnSet = tx.run_single_parameter_query(query, pk)
        counter = 0
        for gene in returnSet:
            counter += 1
            genePrimaryKey = gene['primaryKey']
            modLocalId = gene['modLocalId']
        if counter > 1:
            genePrimaryKey = None
            modLocalId = None
            print ("returning more than one gene: this is an error")

        return genePrimaryKey

