from py2neo import Graph, Node, Relationship

class GeneIndexer:

    def __init__(self, graph):
        self.graph = graph

    def index_genes(self, data):
        tx = self.graph.begin()

        for entry in data:

            gene_node = Node("Gene", primary_key=entry['primaryId'])
            if entry['name'] is not None:
                gene_node['name'] = entry['name']
            if entry['synonyms'] is not None:
                gene_node['synonyms'] = entry['synonyms']
            tx.merge(gene_node, "Gene", "primary_key") # Merge on label "Disease" and property "primary_key".

        tx.commit()