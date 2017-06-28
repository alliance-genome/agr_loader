from py2neo import Graph

class GeneIndexer:

	def __init__(self, graph):
		self.graph = graph
		self.statement = "MERGE (n:Gene)"

	def index_genes(self, data):
		tx = self.graph.begin()

		for entry in data:
			tx.append(self.statement, {"n": entry['primaryId']})
			tx.process()

		tx.commit()