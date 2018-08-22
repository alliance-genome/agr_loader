from .transaction import Transaction

class MolIntTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 3000

    def mol_int_tx(self, data):
        '''
        Loads the Molecular Interaction data into Neo4j.

        '''
        query = """
            UNWIND $data as row 

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
            MERGE (g1)-[iw:INTERACTS_WITH {uuid:row.uuid}]->(g2)

            //Create the Association node to be used for the object.
            MERGE (oa:Association {primaryKey:row.uuid})
                SET oa :InteractionGeneJoin
                SET oa.joinType = 'molecular_interaction'
            MERGE (g1)-[a1:ASSOCIATION]->(oa)
            MERGE (oa)-[a2:ASSOCIATION]->(g2)

            //Create the publication nodes and link them to the Association node.
            MERGE (pn:Publication {primaryKey:row.pub_med_id})
                SET pn.pubMedUrl = row.pub_med_url
                SET pn.pubMedId = row.pub_med_id
            MERGE (oa)-[ev:EVIDENCE]->(pn)

            //Link detection method to the MI ontology.
            MERGE (oa)-[dm:DETECTION_METHOD]->(mi)

            //Link source database to the MI ontology.
            MERGE (oa)-[sd:SOURCE_DATABASE]->(sdb)

            //Link aggregation database to the MI ontology.
            MERGE (oa)-[ad:AGGREGATION_DATABASE]->(adb)

            //Link interactor roles and types to the MI ontology.
            MERGE (oa)-[ita1:INTERACTOR_A_TYPE]->(ita)
            MERGE (oa)-[itb1:INTERACTOR_B_TYPE]->(itb)
            MERGE (oa)-[ira1:INTERACTOR_A_ROLE]->(ira)
            MERGE (oa)-[irb1:INTERACTOR_B_ROLE]->(irb)

            //Link interaction type to the MI ontology.
            MERGE (oa)-[it1:INTERACTION_TYPE]->(it)

            WITH oa, row.interactor_id_and_linkout AS events
                UNWIND events AS event
                    MERGE (id:CrossReference:Identifier {primaryKey:event.primaryKey})
                        SET id.name = event.name
                        SET id.globalCrossRefId = event.globalCrossRefId
                        SET id.localId = event.localId
                        SET id.crossRefCompleteUrl = event.crossRefCompleteUrl
                        SET id.prefix = event.prefix
                        SET id.crossRefType = event.crossRefType
                        SET id.uuid = event.uuid
                        SET id.page = event.page
                        SET id.primaryKey = event.primaryKey
                        SET id.displayName = event.displayName

                    MERGE (oa)-[gcr:CROSS_REFERENCE]->(id) """

        Transaction.execute_transaction_batch(self, query, data, self.batch_size)

        # Transaction.execute_transaction(self, query, data)
        # Transaction.execute_transaction(self, crossReferenceQuery, data)