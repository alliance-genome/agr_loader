from .transaction import Transaction
from services import CreateCrossReference

class AlleleTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def allele_tx(self, data):

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

            FOREACH (entry in row.secondaryIds |
                MERGE (second:SecondaryId:Identifier {primaryKey:entry})
                    SET second.name = entry
                MERGE (o)-[aka1:ALSO_KNOWN_AS]->(second)
                MERGE (l)-[las:LOADED_FROM]-(second))

            FOREACH (entry in row.synonyms |
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                    SET syn.name = entry
                MERGE (o)-[aka2:ALSO_KNOWN_AS]->(syn)
                MERGE (l)-[lasyn:LOADED_FROM]-(syn))

            MERGE (o)-[aspec:FROM_SPECIES]->(spec)
            MERGE (l)-[laspec:LOADED_FROM]-(spec)

            MERGE (o)<-[ag:IS_ALLELE_OF]->(g)
            //Merge the entity node.

            //Create the entity relationship to the gene node.
            MERGE (o)-[c1:CREATED_BY]->(ent)

            WITH o, row.crossReferences AS events
            UNWIND events AS event

        """ + CreateCrossReference.get_cypher_xref_text("allele")

        Transaction.execute_transaction(self, alleleQuery, data)
