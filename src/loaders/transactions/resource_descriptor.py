from .transaction import Transaction


class ResourceDescriptorTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def resource_descriptor_tx(self, data):
        '''
        Loads the resource descriptor data into Neo4j.
        '''


        query = """

            UNWIND $data AS row


        """

        Transaction.execute_transaction(self, query, data)
