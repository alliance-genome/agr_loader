from py2neo import Graph, Node, Relationship

class Association:

	def create_assoc(self, node1, node2, relationship):
		assoc_node = Node("Association", primary_key=relationship)
		assoc_rel_1 = Relationship(node1, "association", assoc_node)
		assoc_rel_2 = Relationship(node2, "association", assoc_node)
		return(assoc_node, assoc_rel_1, assoc_rel_2)