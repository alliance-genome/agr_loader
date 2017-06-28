from py2neo import Graph

class GeneIndexer:

	def __init__(self, graph):
		self.graph = graph
		self.statement = "MERGE (n:Gene)"

	def index_genes(self, data):
		transaction = self.graph.cypher.begin()

		for entry in data:
			tx.append(statement, {"n": entry['primaryId']})
			tx.process()

		tx.commit()