from .transaction import Transaction

class DOTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 2000

    def do_tx(self, data):
        '''
        Loads the DO data into Neo4j.
        '''

        query = """
            UNWIND $data as row

            //Create the DOTerm node and set properties. primaryKey is required.
            MERGE (doterm:DOTerm:Ontology {primaryKey:row.id})
                SET doterm.name = row.name
                SET doterm.nameKey = row.name_key
                SET doterm.definition = row.defText
                SET doterm.defLinks = row.defLinksProcessed
                SET doterm.is_obsolete = row.is_obsolete
                SET doterm.subset = row.subset
                SET doterm.doDisplayId = row.id
                SET doterm.doUrl = row.oUrl
                SET doterm.doPrefix = "DOID"
                SET doterm.doId = row.id
                SET doterm.rgdLink = row.rgd_link
                SET doterm.ratOnlyRgdLink = row.rat_only_rgd_link
                SET doterm.humanOnlyRgdLink = row.human_only_rgd_link
                SET doterm.mgiLink = row.mgi_link
                SET doterm.zfinLink = row.zfin_link
                SET doterm.flybaseLink = row.flybase_link
                SET doterm.wormbaseLink = row.wormbase_link
                SET doterm.sgdLink = "SGD"


            FOREACH (entry in row.o_synonyms |
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                MERGE (doterm)-[aka:ALSO_KNOWN_AS]->(syn))

            FOREACH (isa in row.isas |
                MERGE (doterm2:DOTerm:Ontology {primaryKey:isa})
                MERGE (doterm)-[aka:IS_A]->(doterm2))

        """

        queryXref = """

            UNWIND $data as row
             WITH row.xref_urls AS xrurls
                UNWIND xrurls AS xref
                    MATCH (dt:DOTerm:Ontology {primaryKey:xref.oid})

                    MERGE (cr:CrossReference:Identifier {primaryKey:xref.primaryKey})
                     SET cr.localId = xref.local_id
                     SET cr.prefix = xref.prefix
                     SET cr.crossRefCompleteUrl = xref.complete_url
                     SET cr.name = xref.xrefId
                     SET cr.crossRefType = xref.crossRefType
                     SET cr.uuid = xref.uuid
                     SET cr.globalCrossRefId = xref.globalCrossRefId
                     SET cr.displayName = xref.displayName
                    MERGE (dt)-[aka:CROSS_REFERENCE]->(cr)


        """
        Transaction.execute_transaction_batch(self, query, data, self.batch_size)
        Transaction.execute_transaction_batch(self, queryXref, data, self.batch_size)
