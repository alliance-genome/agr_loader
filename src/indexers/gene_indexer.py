from py2neo import Graph, Node, Relationship

class GeneIndexer:

	def __init__(self, graph):
		self.graph = graph
		self.statement = "MERGE (n:Gene)"

	def index_genes(self, data):
		tx = self.graph.begin()

		for entry in data:
			a = Node("Gene", primary_key=entry['primaryId'])
			tx.create(a)

		tx.commit()