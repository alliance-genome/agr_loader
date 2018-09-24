# coding=utf-8
from .transaction import Transaction


class DiseaseGeneTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def gene_disease_ortho_tx(self, data):
        # Loads the gene to disease via ortho  data into Neo4j.

        executeG2D = """

            MATCH 


            """

        Transaction.execute_transaction(self, executeG2D, data)
