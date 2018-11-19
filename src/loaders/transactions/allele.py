from .transaction import Transaction
from services import CreateCrossReference

class AlleleTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def allele_tx(self, allele_data, secondary_data, synonym_data, xref_data):

        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(data)
        # quit()

        #TODO: make one query for all xref stanzas instead of duplicating in 5 different files: go.py, do.py, bgi.py, allele.py, geo_xref.py

        alleleQuery = """

            UNWIND $data AS row

            MATCH (g:Gene {primaryKey: row.geneId})
            MATCH (s:Species {primaryKey: row.taxonId})

            //Create the load node(s)
            MERGE (l:Load:Entity {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced,
                 l.loadName = "Allele",
                 l.release = row.release,
                 l.dataProviders = row.dataProviders,
                 l.dataProvider = row.dataProvider

            //Create the Allele node and set properties. primaryKey is required.
            MERGE (o:Feature {primaryKey:row.primaryId})
                SET o.symbol = row.symbol,
                 o.taxonId = row.taxonId,
                 o.dateProduced = row.dateProduced,
                 o.release = row.release,
                 o.localId = row.localId,
                 o.globalId = row.globalId,
                 o.uuid = row.uuid,
                 o.symbolText = row.symbolText,
                 o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                 o.dataProviders = row.dataProviders,
                 o.dataProvider = row.dataProvider

            MERGE (o)-[:FROM_SPECIES]-(s)

            //FOREACH (dataProvider in row.dataProviders |
                //MERGE (dp:DataProvider:Entity {primaryKey:dataProvider})
                  //SET dp.dateProduced = row.dateProduced
                //MERGE (o)-[odp:DATA_PROVIDER]-(dp)
            MERGE (l)-[lo:LOADED_FROM]-(o)


            MERGE (o)-[aspec:FROM_SPECIES]->(spec)
            MERGE (l)-[laspec:LOADED_FROM]-(spec)

            MERGE (o)<-[ag:IS_ALLELE_OF]->(g)
            //Merge the entity node.

            //Create the entity relationship to the gene node.
            MERGE (o)-[c1:CREATED_BY]->(ent)

        """

        allele_secondaryIds = """

         UNWIND $data AS row
                MATCH (f:Feature {primaryKey:row.data_id})

                MERGE (second:SecondaryId:Identifier {primaryKey:row.secondary_id})
                    SET second.name = row.secondary_id
                MERGE (f)-[aka1:ALSO_KNOWN_AS]->(second)


        """
        allele_synonyms = """

         UNWIND $data AS row
                MATCH (f:Feature {primaryKey:row.data_id})

               MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                    SET syn.name = row.synonym
                MERGE (f)-[aka2:ALSO_KNOWN_AS]->(syn)
        """

        allele_xrefs = """
            UNWIND $data as event
                MATCH (o:Feature {primaryKey:event.dataId})
        
        """ + CreateCrossReference.get_cypher_xref_text("feature")

        if len(allele_data) > 0:
            Transaction.execute_transaction(self, alleleQuery, allele_data)
        if len(secondary_data) > 0:
            Transaction.execute_transaction(self, allele_secondaryIds, secondary_data)
        if len(synonym_data) > 0:
            Transaction.execute_transaction(self, allele_synonyms, synonym_data)
        if len(xref_data) > 0:
            Transaction.execute_transaction(self, allele_xrefs, xref_data)



