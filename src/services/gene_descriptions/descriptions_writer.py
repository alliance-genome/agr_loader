from genedescriptions.descriptions_writer import DescriptionsWriter
from services.gene_descriptions.data_fetcher import Neo4jDataFetcher


class Neo4jGDWriter(DescriptionsWriter):
    """write gene descriptions to AGR neo4j database"""

    def __init__(self):
        super().__init__()

    def write(self, db_graph):
        query = """
            UNWIND $descriptions as row 

            MATCH (g:Gene {primaryKey:row.gene_id})
                SET g.automatedGeneSynopsis = row.description
            """
        Neo4jDataFetcher.query_db(db_graph=db_graph, query=query, parameters={
            "descriptions": [{
                "gene_id": gene_desc.gene_id,
                "description": gene_desc.description
            } for gene_desc in self.data]})
