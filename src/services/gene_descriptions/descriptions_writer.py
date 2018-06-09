from genedescriptions.descriptions_writer import DescriptionsWriter
from loaders.transactions.gene_description import GeneDescriptionTransaction


class Neo4jGDWriter(DescriptionsWriter):
    """write gene descriptions to AGR neo4j database"""

    def __init__(self):
        super().__init__()

    def write(self, db_graph):
        data = [{"gene_id": gene_desc.gene_id,
                 "description": gene_desc.description,
                 "go_description": gene_desc.go_description,
                 "disease_description": gene_desc.disease_description} for gene_desc in self.data]
        tx = GeneDescriptionTransaction(db_graph)
        tx.gd_tx(data)
