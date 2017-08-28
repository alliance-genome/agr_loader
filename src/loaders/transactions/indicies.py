from neo4j.v1 import GraphDatabase

class Indicies():

    def __init__(self, graph):
        self.graph = graph

    def create_indicies(self):
        session = self.graph.session()
        session.run("CREATE INDEX ON :Gene(primaryKey)")
        session.run("CREATE INDEX ON :GOTerm(primaryKey)")
        session.run("CREATE INDEX ON :Genotype(primaryKey)")
        session.run("CREATE INDEX ON :SOTerm(primaryKey)")
        session.run("CREATE INDEX ON :Ontology(primaryKey)")
        session.run("CREATE INDEX ON :DOTerm(primaryKey)")
        session.run("CREATE INDEX ON :Publication(primaryKey)")
        session.run("CREATE INDEX ON :EvidenceCode(primaryKey)")
        session.run("CREATE INDEX ON :Allele(primaryKey)")
        session.run("CREATE INDEX ON :Transgene(primaryKey)")
        session.run("CREATE INDEX ON :Fish(primaryKey)")
        session.run("CREATE INDEX ON :DiseaseObject(primaryKey)")
        session.run("CREATE INDEX ON :LocationObject(primaryKey)")
        session.run("CREATE INDEX ON :Location(primaryKey)")
        session.run("CREATE INDEX ON :EnvironmentCondition(primaryKey)")
        session.run("CREATE INDEX ON :Environment(primaryKey)")
        session.run("CREATE INDEX ON :Species(primaryKey)")
        session.run("CREATE INDEX ON :Annotation(primaryKey)")
        session.run("CREATE INDEX ON :Entity(primaryKey)")
        session.run("CREATE INDEX ON :Synonym(primaryKey)")
        session.run("CREATE INDEX ON :Identifier(primaryKey)")
        session.run("CREATE INDEX ON :ExternalId(primaryKey)")
        session.run("CREATE INDEX ON :Association(primaryKey)")
        session.run("CREATE INDEX ON :CrossReference(primaryKey)")
        session.run("CREATE INDEX ON :SecondaryId(primaryKey)")
        session.run("CREATE INDEX ON :Chromosome(primaryKey)")
        session.run("CREATE INDEX ON :OrthoAlgorithm (name)")

        session.close()

    # Property constraints require Enterprise Edition. :(
    # def create_constraints(self):
    #     session = self.graph.session()
    #     session.run("CREATE CONSTRAINT ON (g:Gene) ASSERT g.primaryKey IS UNIQUE")
    #     session.run("CREATE CONSTRAINT ON (g:Gene) ASSERT exists(g.primaryKey)")

    #     session.run("CREATE CONSTRAINT ON (go:GOTerm) ASSERT go.primaryKey IS UNIQUE")
    #     session.run("CREATE CONSTRAINT ON (go:GOTerm) ASSERT exists(go.primaryKey)")
    #     session.close()
