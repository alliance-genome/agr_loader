from py2neo import Graph, Node, Relationship
from .transaction import Transaction

class GeneIndexer:

    def __init__(self, graph):
        self.graph = graph

    def index_genes(self, data):
        tx = Transaction(self.graph)
        
        label = "Gene"
        nodes = data
        primary_key = "primaryId"
                
        tx.batch_merge_simple(self, label, data, primary_key)

        # tx = self.graph.begin()

        # for entry in data:

        #     gene_node = Node("Gene", primary_key=entry['primaryId'])
        #     if entry['name'] is not None:
        #         gene_node['name'] = entry['name']
        #     if entry['synonyms'] is not None:
        #         gene_node['synonyms'] = entry['synonyms']
        #     tx.merge(gene_node, "Gene", "primary_key") # Merge on label "Gene" and property "primary_key".

        # tx.commit()