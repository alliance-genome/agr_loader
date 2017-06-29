from py2neo import Graph, Node, Relationship

class DOIndexer:

    def __init__(self, graph):
        self.graph = graph

    def index_do(self, data):
        print("Loading disease information into Neo4J.")
        tx = self.graph.begin()
        load_size = 0

        for entry in data:
            load_size += 1
            do_node = Node("Disease", primary_key=entry)
            tx.merge(do_node, "Disease", "primary_key") # Merge on label "Disease" and property "primary_key".
            
            if load_size == 5000:
                tx.commit()
                print("Loaded %s nodes..." % load_size)
                load_size = 0
                tx = self.graph.begin()
        
        print("Loaded %s nodes..." % load_size)
        tx.commit()

    def annotate_do(self, data):
        print("Loading disease annotations into Neo4J.")
        tx = self.graph.begin()
        load_size = 0

        for entry in data:
            gene_node = Node("Gene", primary_key=entry)
            for sub_entry in data[entry]:
                do_node = Node("Disease", primary_key=sub_entry['do_id'])
                gene_node_to_do_node = Relationship(gene_node, "model_of", do_node)
                tx.merge(gene_node_to_do_node)
                load_size += 1

            if load_size == 5000:
                tx.commit()
                print("Loaded %s edges..." % load_size)
                load_size = 0
                tx = self.graph.begin()
        
        print("Loaded %s edges..." % load_size)
        tx.commit()