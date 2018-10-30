from genedescriptions.descriptions_writer import DescriptionsWriter
from transactions.gene_description import GeneDescriptionTransaction


class Neo4jGDWriter(DescriptionsWriter):
    """write gene descriptions to AGR neo4j database"""

    def __init__(self):
        super().__init__()

    def write_neo4j(self):
        data = [{"gene_id": gene_desc.gene_id,
                 "description": gene_desc.description} for gene_desc in self.data]
        tx = GeneDescriptionTransaction()
        tx.gd_tx(data)
