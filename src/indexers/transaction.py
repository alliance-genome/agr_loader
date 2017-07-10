from py2neo import Graph

class Transaction():

    def __init__(self, graph):
        self.graph = graph

    def batch_merge_simple(self, label, nodes, primary_key)
        # Nodes should be a simple list of dictionaries with one entry defining the primary key.
        query = (
            "UNWIND %s as row \
            MERGE (n:%s /{primary_key:%s/})"
        % nodes, label, primary_key)

        self.graph.run(query)

            # 'UNWIND %s as row '
            # 'MERGE (n:Label /{row.id/}) '
            # '(ON CREATE) SET n += row.properties '