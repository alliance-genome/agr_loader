from .transaction import Transaction

class IMEXTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)
        self.batch_size = 3000

    def imex_tx(self, data):
        '''
        Loads the IMEX data into Neo4j.

        '''
        query = """
            UNWIND $data as row 

            MATCH (s1:Species {primaryKey:taxon_id_1})-[fs1:FROM_SPECIES]-(g1:Gene)-[x1:CrossReference]-(y1:CrossReference {globalCrossRefId:row.interactor_one})
            MATCH (s2:Species {primaryKey:taxon_id_2})-[fs2:FROM_SPECIES]-(g2:Gene)-[x2:CrossReference]-(y2:CrossReference {globalCrossRefId:row.interactor_two})

            //Create the relationship between the two genes
            MERGE (g1)-[iw:INTERACTS_WITH {uuid:row.uuid}]->(g2)

            //Create the Association node to be used for the object/doTerm
            MERGE (oa:Association {primaryKey:row.uuid})
                SET oa :InteractionGeneJoin
                SET oa.joinType = 'interaction'
            MERGE (g1)-[a1:ASSOCIATION]->(oa)
            MERGE (oa)-[a2:ASSOCIATION]->(g2)

            //Create the additional nodes to hang off the Association node.
            MERGE (ed:ExperimentalDetails)
                SET ed.detectionMethod = row.detection_method
                SET ed.molecule_type = row.molecule_type
            MERGE (oa)-[si:SUPPORTING_INFORMATION]-(ed)

            




        """
        Transaction.execute_transaction_batch(self, query, data, self.batch_size)
        #Transaction.execute_transaction_batch(self, queryXref, data, self.batch_size)

        #imex_dataset = {
        #     'interactor_one' : interactor_one,
        #     'interactor_two' : interactor_two,
        #     'interactor_type' : interactor_type,
        #     'molecule_type' : molecule_type,
        #     'taxon_id_1' : taxon_id_1_to_load,
        #     'taxon_id_2' : taxon_id_2_to_load,
        #     'detection_method' : detection_method,
        #     'pub_med_id' : publication,
        #     'pub_med_url' : publication_url
        #     'uuid' : str(uuid.uuid4())
        # }