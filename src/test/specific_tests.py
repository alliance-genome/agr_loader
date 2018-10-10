from neo4j.v1 import GraphDatabase
import os


def execute_transaction(query):
    host = os.environ['NEO4J_NQC_HOST']
    port = os.environ['NEO4J_NQC_PORT']
    uri = "bolt://" + host + ":" + port
    graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))

    result = None

    with graph.session() as session:
        result = session.run(query)

    return result


def test_fgf8a_exists():
    query = "MATCH (g:Gene) WHERE g.symbol = 'fgf8a' RETURN count(g) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0

# def test_hip1_exists():
#     query = "MATCH (g:Gene) WHERE g.symbol = 'Hip1' RETURN count(g) AS count"
#     result = execute_transaction(query)
#     for record in result:
#         assert record["count"] > 0


def test_doterm_exists():
    query = "MATCH(n:DOTerm) where n.primaryKey = 'DOID:0001816' RETURN count(n) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] == 1


def test_isobsolete_false():
    query = "MATCH(n:DOTerm) where n.is_obsolete = 'false' RETURN count(n) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_species_disease_pub_gene_exists():
    query = "MATCH (s:Species)--(g:Gene)--(dg:DiseaseEntityJoin)--(p:Publication) RETURN COUNT(p) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_species_disease_pub_allele_exists():
    query = "MATCH (s:Species)--(f:Feature)--(dg:DiseaseEntityJoin)--(p:Publication) RETURN COUNT(p) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_uuid_is_not_duplicated():
    query = "MATCH (g) WITH g.uuid AS uuid, count(*) " \
            "AS counter WHERE counter > 0 AND g.uuid IS NOT NULL RETURN uuid, counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 2


def test_zfin_gene_has_expression_link():
    query = "MATCH (g:Gene)-[]-(c:CrossReference) " \
            "where g.primaryKey = 'ZFIN:ZDB-GENE-990415-72' " \
            "and c.crossRefType = 'gene/expression' return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_xref_complete_url_is_formatted():
    query = "MATCH (cr:CrossReference) where not cr.crossRefCompleteUrl =~ 'http.*' " \
            "and cr.crossRefType <> 'ontology_provided_cross_reference' return count(cr) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_spell_display_name():
    query = "MATCH (cr:CrossReference) where cr.prefix = 'SPELL' " \
            "and cr.displayName <> 'Serial Patterns of Expression Levels Locator (SPELL)' return count(cr) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_spell_crossRefType():
    query = "MATCH (cr:CrossReference) where cr.prefix = 'SPELL' " \
            "and cr.crossRefType <> 'gene/spell' return count(cr) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_gene_has_automated_description():
    query = "MATCH (g:Gene) where g.primaryKey = 'ZFIN:ZDB-GENE-030131-4430' " \
            "and g.automatedGeneSynopsis is not null return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_nephrogenic_diabetes_insipidus_has_at_least_one_gene():
    query = "MATCH (d:DOTerm)-[]-(g:Gene) where d.name = 'nephrogenic diabetes insipidus' return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_ZDB_ALT_160129_6_has_at_least_one_disease():
    query = "MATCH (d:DOTerm)-[]-(f:Feature) where f.dataProvider = 'ZFIN' " \
            "and f.primaryKey ='ZFIN:ZDB-ALT-160129-6' return count(f) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_do_terms_have_parents():
    query = "MATCH (d:DOTerm) WHERE NOT (d)-[:IS_A]->() " \
            "and d.is_obsolete = 'false' and d.doId <> 'DOID:4' return count(d) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1

        
def test_every_species_has_phenotype_has_pub():
    query = "MATCH (s:Species)--()-[hp:HAS_PHENOTYPE]-(p:Phenotype)-[]-(pa:PhenotypeEntityJoin)-[]-(pub:Publication) " \
            "RETURN count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_phenotype_for_all_species_exists():
    query = "MATCH (s:Species)--()-[hp:HAS_PHENOTYPE]-(p:Phenotype) RETURN count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_expression_for_non_human_species_exists():
    query = "MATCH (s:Species)--()-[hp:EXPRESSED_IN]-(e:ExpressionBioEntity) RETURN count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 6


def test_cellular_component_relationship_for_expression_exists():
    query = "MATCH (n:ExpressionBioEntity)-[r:CELLULAR_COMPONENT]-(g:GOTerm) return count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_structure_relationship_for_expression_exists():
    query = "MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_STRUCTURE]-(o:Ontology) RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_sub_structure_relationship_for_expression_exists():
    query = "MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_SUB_SUBSTRUCTURE]-(o:Ontology) RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_structure_qualifier_relationship_for_expression_exists():
    query = "MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_STRUCTURE_QUALIFIER]-(o:Ontology) RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_cellular_component_qualifier_relationship_for_expression_exists():
    query = "MATCH (n:ExpressionBioEntity)-[r:CELLULAR_COMPONENT_QUALIFIER]-(o:Ontology) RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_sub_structure_qualifier_relationship_for_expression_exists():
    query = "MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_SUB_STRUCTURE_QUALIFIER]-(o:Ontology) " \
            "RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_structure_uberon_relationship_for_expression_exists():
    query = "MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_RIBBON_TERM]-(o:UBERONTerm:Ontology) " \
            "where o.primaryKey <> 'UBERON:AnatomyOtherLocation'" \
            "RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_structure_uberon_other_relationship_for_expression_exists():
    query = "MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_RIBBON_TERM]-(o:UBERONTerm:Ontology) " \
            "where o.primaryKey = 'UBERON:AnatomyOtherLocation'" \
            "RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gocc_other_relationship_for_expression_exists():
    query = "MATCH (n:ExpressionBioEntity)-[r:CELLULAR_COMPONENT_RIBBON_TERM]-(o:GOTerm:Ontology) " \
            "where o.primaryKey = 'GO:otherLocations'" \
            "RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gocc_ribbon_relationship_for_expression_exists():
    query = "MATCH (n:ExpressionBioEntity)-[r:CELLULAR_COMPONENT_RIBBON_TERM]-(o:GOTerm:Ontology) " \
            "where o.primaryKey <> 'GO:otherLocations'" \
            "RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_stage_uberon_other_relationship_for_expression_exists():
    query = "MATCH (n:BioEntityGeneExpressionJoin)-[r:STAGE_RIBBON_TERM]-(o:UBERONTerm:Ontology) " \
            "RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_stage_uberon_relationship_for_expression_exists():
    query = "MATCH (n:BioEntityGeneExpressionJoin)-[r:STAGE_RIBBON_TERM]-(o:UBERONTerm:Ontology) " \
            "RETURN count(r) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_mmoterm_has_display_synonym():
    query = "MATCH (n:MMOTerm) where n.primaryKey = 'MMO:0000658' and n.display_synonym = 'RNA in situ'" \
            "RETURN count(n) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_crip2_has_cardiac_neural_crest():
    query = "MATCH (gene:Gene)--(ebe:ExpressionBioEntity)--(ei:BioEntityGeneExpressionJoin)--(pub:Publication)" \
            "where ebe.whereExpressedStatement = 'cardiac neural crest'"\
            "and gene.primaryKey = 'ZFIN:ZDB-GENE-040426-2889'" \
            "and pub.pubModId = 'ZFIN:ZDB-PUB-130309-4' return count(gene) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_feature_has_mod_url():
    query = "MATCH (feature:Feature) where not feature.modCrossRefCompleteUrl =~ 'http.*' " \
            "return count(feature) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1
