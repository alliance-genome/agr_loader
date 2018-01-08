from .transaction import Transaction

class AlleleTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def allele_tx(self, data):

        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(data)
        # quit()

        alleleQuery = """

            UNWIND $data AS row

            MATCH (g:Gene {primaryKey: row.geneId})
            MATCH (s:Species {primaryKey: row.taxonId})

            //Create the load node(s)
            MERGE (l:Load {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced
                SET l.dataProvider = row.dataProvider
                SET l.loadName = "Allele"

            //Create the Allele node and set properties. primaryKey is required.
            MERGE (a:Feature {primaryKey:row.primaryId})
                SET a.symbol = row.symbol
                SET a.taxonId = row.taxonId
                SET a.dateProduced = row.dateProduced
                SET a.dataProvider = row.dataProvider
                SET a.release = row.release
                SET a.localId = row.localId
                SET a.globalId = row.globalId

            FOREACH (entry in row.secondaryIds |
                MERGE (second:SecondaryId:Identifier {primaryKey:entry})
                    SET second.name = entry
                MERGE (a)-[aka1:ALSO_KNOWN_AS]->(second)
                MERGE (l)-[las:LOADED_FROM]-(second))

            FOREACH (entry in row.synonyms |
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                    SET syn.name = entry
                MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn)
                MERGE (l)-[lasyn:LOADED_FROM]-(syn))

            MERGE (a)-[:FROM_SPECIES]->(spec)
            MERGE (l)-[laspec:LOADED_FROM]-(spec)

            MERGE (a)<-[ag:IS_ALLELE_OF]->(g)

        """

        Transaction.execute_transaction(self, alleleQuery, data)
