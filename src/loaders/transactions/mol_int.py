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

            MATCH (g1:Gene {primaryKey:row.interactor_A}),
             (g2:Gene {primaryKey:row.interactor_B}),
             ((mi:MITerm:Ontology) WHERE mi.primaryKey = row.detection_method),
             ((sdb:MITerm:Ontology) WHERE sdb.primaryKey = row.source_database),
             ((adb:MITerm:Ontology) WHERE adb.primaryKey = row.aggregation_database),
             ((ita:MITerm:Ontology) WHERE ita.primaryKey = row.interactor_A_type),
             ((itb:MITerm:Ontology) WHERE itb.primaryKey = row.interactor_B_type),
             ((ira:MITerm:Ontology) WHERE ira.primaryKey = row.interactor_A_role),
             ((irb:MITerm:Ontology) WHERE irb.primaryKey = row.interactor_B_role),
             ((it:MITerm:Ontology) WHERE it.primaryKey = row.interaction_type)

            //Create the relationship between the two genes.
            CREATE (g1:Gene)-[iw:INTERACTS_WITH {uuid:row.uuid}]->(g2:Gene)

            //Create the Association node to be used for the object.
            MERGE (oa:Association:InteractionGeneJoin {primaryKey:row.uuid})
                SET oa.joinType = 'molecular_interaction'
            MERGE (g1:Gene)-[a1:ASSOCIATION]->(oa:Association:InteractionGeneJoin)
            MERGE (oa:Association:InteractionGeneJoin)-[a2:ASSOCIATION]->(g2:Gene)

            //Create the publication nodes and link them to the Association node.
            MERGE (pn:Publication {primaryKey:row.pub_med_id})
                SET pn.pubMedUrl = row.pub_med_url
                SET pn.pubMedId = row.pub_med_id
            MERGE (oa:Association:InteractionGeneJoin)-[ev:EVIDENCE]->(pn:Publication)

            //Link detection method to the MI ontology.
            MERGE (oa:Association:InteractionGeneJoin)-[dm:DETECTION_METHOD]->(mi:MITerm)

            //Link source database to the MI ontology.
            MERGE (oa:Association:InteractionGeneJoin)-[sd:SOURCE_DATABASE]->(sdb:MITerm:Ontology)

            //Link aggregation database to the MI ontology.
            MERGE (oa:Association:InteractionGeneJoin)-[ad:AGGREGATION_DATABASE]->(adb:MITerm:Ontology)

            //Link interactor roles and types to the MI ontology.
            MERGE (oa:Association:InteractionGeneJoin)-[ita1:INTERACTOR_A_TYPE]->(ita:MITerm:Ontology)
            MERGE (oa:Association:InteractionGeneJoin)-[itb1:INTERACTOR_B_TYPE]->(itb:MITerm:Ontology)
            MERGE (oa:Association:InteractionGeneJoin)-[ira1:INTERACTOR_A_ROLE]->(ira:MITerm:Ontology)
            MERGE (oa:Association:InteractionGeneJoin)-[irb1:INTERACTOR_B_ROLE]->(irb:MITerm:Ontology)

            //Link interaction type to the MI ontology.
            MERGE (oa:Association:InteractionGeneJoin)-[it1:INTERACTION_TYPE]->(it:MITerm:Ontology)

            WITH oa, row.interactor_id_and_linkout AS events
                UNWIND events AS event
                    MERGE (id:CrossReference:Identifier {primaryKey:event.primaryKey})
                        SET id.name = event.name,
                         id.globalCrossRefId = event.globalCrossRefId,
                         id.localId = event.localId,
                         id.crossRefCompleteUrl = event.crossRefCompleteUrl,
                         id.prefix = event.prefix,
                         id.crossRefType = event.crossRefType,
                         id.uuid = event.uuid,
                         id.page = event.page,
                         id.primaryKey = event.primaryKey,
                         id.displayName = event.displayName

                    MERGE (oa:Association:InteractionGeneJoin)-[gcr:CROSS_REFERENCE]->(id:CrossReference:Identifier) """

        Transaction.execute_transaction_batch(self, query, data, self.batch_size)

        # Transaction.execute_transaction(self, query, data)
        # Transaction.execute_transaction(self, crossReferenceQuery, data)