from neo4j.v1 import GraphDatabase

class Indicies():

    def __init__(self, graph):
        self.graph = graph

    def create_indicies(self):
        session = self.graph.session()
        session.run("CREATE INDEX ON :Gene(primaryKey)")
        session.run("CREATE INDEX ON :GOTerm(primaryKey)")
        session.close()

    # Property constraints require Enterprise Edition. :(
    # def create_constraints(self):
    #     session = self.graph.session()
    #     session.run("CREATE CONSTRAINT ON (g:Gene) ASSERT g.primaryKey IS UNIQUE")
    #     session.run("CREATE CONSTRAINT ON (g:Gene) ASSERT exists(g.primaryKey)")

    #     session.run("CREATE CONSTRAINT ON (go:GOTerm) ASSERT go.primaryKey IS UNIQUE")
    #     session.run("CREATE CONSTRAINT ON (go:GOTerm) ASSERT exists(go.primaryKey)")
    #     session.close()