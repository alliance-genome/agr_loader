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

            //Lookup genes based on species and uniprot ids.
            MATCH (g1:Gene {primaryKey:row.interactor_A})
            MATCH (g2:Gene {primaryKey:row.interactor_B})
            MATCH (mi:MITerm) WHERE mi.primaryKey = row.detection_method

            //Create the relationship between the two genes.
            MERGE (g1)-[iw:INTERACTS_WITH {uuid:row.uuid}]->(g2)

            //Create the Association node to be used for the object.
            MERGE (oa:Association {primaryKey:row.uuid})
                SET oa :InteractionGeneJoin
                SET oa.joinType = 'molecular_interaction'
            MERGE (g1)-[a1:ASSOCIATION]->(oa)
            MERGE (oa)-[a2:ASSOCIATION]->(g2)

            //Create the additional nodes to hang off the Association node.
            MERGE (ed:ExperimentalDetails {interactorType:row.interactor_type, moleculeType:row.molecule_type})
            MERGE (oa)-[si:SUPPORTING_INFORMATION]->(ed)

            //Create the publication nodes and link them to the Association node.
            MERGE (pn:Publication {primaryKey:row.pub_med_id})
                SET pn.pubMedUrl = row.pub_med_url
                SET pn.pubMedId = row.pub_med_id
            MERGE (oa)-[ev:EVIDENCE]->(pn)

            //Link detection method to the MI ontology.
            MERGE (oa)-[dm:DETECTION_METHOD]->(mi)

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