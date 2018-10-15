from .transaction import Transaction

class MolIntTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 3000

    def mol_int_tx(self):
        '''
        Loads the Molecular Interaction data into Neo4j.

        '''
        query = """
            USING PERIODIC COMMIT 10000
            LOAD CSV WITH HEADERS FROM \'file:///interactions.csv\' AS row

            MATCH (g1:Gene {primaryKey:row.interactor_A})
            MATCH (g2:Gene {primaryKey:row.interactor_B})

            MATCH (mi:MITerm) WHERE mi.primaryKey = row.detection_method
            MATCH (sdb:MITerm) WHERE sdb.primaryKey = row.source_database
            MATCH (adb:MITerm) WHERE adb.primaryKey = row.aggregation_database
            MATCH (ita:MITerm) WHERE ita.primaryKey = row.interactor_A_type
            MATCH (itb:MITerm) WHERE itb.primaryKey = row.interactor_B_type
            MATCH (ira:MITerm) WHERE ira.primaryKey = row.interactor_A_role
            MATCH (irb:MITerm) WHERE irb.primaryKey = row.interactor_B_role
            MATCH (it:MITerm) WHERE it.primaryKey = row.interaction_type

            //Create the relationship between the two genes.
            CREATE (g1)-[iw:INTERACTS_WITH {uuid:row.uuid}]->(g2)

            //Create the Association node to be used for the object.
            CREATE (oa:Association {primaryKey:row.uuid})
                SET oa :InteractionGeneJoin
                SET oa.joinType = 'molecular_interaction'
            CREATE (g1)-[a1:ASSOCIATION]->(oa)
            CREATE (oa)-[a2:ASSOCIATION]->(g2)

            //Create the publication nodes and link them to the Association node.
            MERGE (pn:Publication {primaryKey:row.pub_med_id})
                ON CREATE SET pn.pubMedUrl = row.pub_med_url,
                pn.pubMedId = row.pub_med_id
            CREATE (oa)-[ev:EVIDENCE]->(pn)

            //Link detection method to the MI ontology.
            CREATE (oa)-[dm:DETECTION_METHOD]->(mi)

            //Link source database to the MI ontology.
            CREATE (oa)-[sd:SOURCE_DATABASE]->(sdb)

            //Link aggregation database to the MI ontology.
            CREATE (oa)-[ad:AGGREGATION_DATABASE]->(adb)

            //Link interactor roles and types to the MI ontology.
            CREATE (oa)-[ita1:INTERACTOR_A_TYPE]->(ita)
            CREATE (oa)-[itb1:INTERACTOR_B_TYPE]->(itb)
            CREATE (oa)-[ira1:INTERACTOR_A_ROLE]->(ira)
            CREATE (oa)-[irb1:INTERACTOR_B_ROLE]->(irb)

            //Link interaction type to the MI ontology.
            CREATE (oa)-[it1:INTERACTION_TYPE]->(it)

            """

        xref_query = """
            USING PERIODIC COMMIT 10000
            LOAD CSV WITH HEADERS FROM \'file:///xref_interactions.csv\' AS row

            // This needs to be a MERGE below.
            MATCH (oa:InteractionGeneJoin :Association) WHERE oa.primaryKey = row.reference_uuid
                MERGE (id:CrossReference:Identifier {primaryKey:row.primaryKey})
                    ON CREATE SET id.name = row.name,
                        id.globalCrossRefId = row.globalCrossRefId,
                        id.localId = row.localId,
                        id.crossRefCompleteUrl = row.crossRefCompleteUrl,
                        id.prefix = row.prefix,
                        id.crossRefType = row.crossRefType,
                        id.uuid = row.uuid,
                        id.page = row.page,
                        id.primaryKey = row.primaryKey,
                        id.displayName = row.displayName

                MERGE (oa)-[gcr:CROSS_REFERENCE]->(id) """

        Transaction.load_csv_data(self, query)
        Transaction.load_csv_data(self, xref_query)
