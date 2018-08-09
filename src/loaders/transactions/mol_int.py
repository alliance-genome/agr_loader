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
            WITH oa, row.detection_method as detection_method
                MATCH (mi:MITerm) WHERE mi.primaryKey = detection_method
                MERGE (oa)-[dm:DETECTION_METHOD]->(mi)
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

        # {
        # 'interactor_one': 'UniProtKB:O43426', 
        # 'interactor_two': 'UniProtKB:P49418',
        # 'interactor_type': 'protein',
        # 'molecule_type': 'protein',
        # 'taxon_id_1': 'NCBITaxon:9606',
        # 'taxon_id_2': 'NCBITaxon:9606',
        # 'detection_method': 'MI:0084',
        # 'pub_med_id': 'PMID:10542231', 
        # 'pub_med_url': 'https://www.ncbi.nlm.nih.gov/pubmed/10542231',
        # 'uuid': 'b02cb6f5-4cd1-4119-acfd-4ca50f42ec73', 
        # }