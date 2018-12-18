from etl import Neo4jHelper


def execute_transaction(query):
    return Neo4jHelper.run_single_query(query)


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


def test_gene_has_all_three_automated_description_components():
    query = "MATCH (g:Gene) where g.primaryKey in ['SGD:S000004695', 'SGD:S000004916', " \
            "'SGD:S000004646', 'SGD:S000000253', 'SGD:S000000364', 'SGD:S000002284'," \
               "'SGD:S000004603', 'SGD:S000004802', 'SGD:S000005707', 'SGD:S000001596'," \
               "'SGD:S000004777', 'SGD:S000006074','SGD:S000002678', 'SGD:S000003487', "\
               "'SGD:S000000458', 'SGD:S000006068', 'WB:WBGene00003412', 'WB:WBGene00000227', 'WB:WBGene00006844'," \
               "'ZFIN:ZDB-GENE-990415-131', 'ZFIN:ZDB-GENE-050517-20', 'ZFIN:ZDB-GENE-040426-1294',"\
               "'ZFIN:ZDB-GENE-040426-1294', 'FB:FBgn0027655', 'FB:FBgn0045035','RGD:68337', 'RGD:2332', " \
               "'MGI:96067', 'MGI:88388', 'MGI:107202', 'MGI:106658', 'MGI:105043'," \
               "'HGNC:4851', 'HGNC:1884', 'HGNC:795', 'HGNC:11291','RGD:1593265', 'RGD:1559787'] " \
            "and not (g.automatedGeneSynopsis =~ '.*xhibits.*'" \
                "or g.automatedGeneSynopsis =~ '.*nvolved in.*'" \
                "or g.automatedGeneSynopsis =~ '.*ocalizes to.*'" \
                "or g.automatedGeneSynopsis =~ '.*redicted to have.*'" \
                "or g.automatedGeneSynopsis =~ '.*redicted to be involved in.*')" \
            "and not g.automatedGeneSynopsis =~ '.*sed to study.*'" \
            "and not g.automatedGeneSynopsis =~ '.*rthologous to.*' return count(g) as counter"
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
            "and d.primaryKey =~ 'DO:.*'" \
            "and d.is_obsolete = 'false' and d.doId <> 'DOID:4' return count(d) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_every_species_has_phenotype_has_pub():
    query = "MATCH (s:Species)--()-[hp:HAS_PHENOTYPE]-(p:Phenotype)-[]-(pa:PhenotypeEntityJoin)-[]-(pub:Publication) " \
            "RETURN count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 6


def test_phenotype_for_all_species_exists():
    query = "MATCH (s:Species)--()-[hp:HAS_PHENOTYPE]-(p:Phenotype) RETURN count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 6


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
            "where ebe.whereExpressedStatement = 'cardiac neural crest'" \
            "and gene.primaryKey = 'ZFIN:ZDB-GENE-040426-2889'" \
            "and pub.pubModId = 'ZFIN:ZDB-PUB-130309-4' return count(gene) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_expression_gocc_other_term_for_specific_gene_exists():
    query = "match (g:Gene)--(ebe:ExpressionBioEntity)-[cc:CELLULAR_COMPONENT]-(go:GOTerm), " \
            "(ebe)-[cr:CELLULAR_COMPONENT_RIBBON_TERM]-(got:GOTerm) where g.primaryKey = 'RGD:2129' " \
            "and ebe.whereExpressedStatement = 'vesicle lumen' and got.primaryKey = 'GO:otherLocations' " \
            "return count(got) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_expression_gocc_term_for_specific_gene_exists():
    query = "match (g:Gene)--(ebe:ExpressionBioEntity)-[cc:CELLULAR_COMPONENT]-(go:GOTerm) " \
            "where g.primaryKey = 'RGD:2129' " \
            "and ebe.whereExpressedStatement = 'vesicle lumen'" \
            "return count(go) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_gocc_other_has_type():
    query = "match (go:GOTerm) where go.subset = 'goslim_agr' and go.type = 'other'" \
            "return count(go) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_gocc_self_ribbon_term_exists():
    query = "match (gene:Gene)--(ebe:ExpressionBioEntity)-[c:CELLULAR_COMPONENT_RIBBON_TERM]-(got:GOTerm) " \
            "where gene.primaryKey = 'ZFIN:ZDB-GENE-140619-1'" \
            "and got.primaryKey = 'GO:0005739' return count(gene) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_annotation_via_ortho_has_biomarker_relation():
    query = "match (gene:Gene)-[r:BIOMARKER_VIA_ORTHOLOGY]-(do:DOTerm) " \
            "return count(gene) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_annotation_via_ortho_has_implicated_relation():
    query = "match (gene:Gene)-[r:IMPLICATED_VIA_ORTHOLOGY]-(do:DOTerm) " \
            "return count(gene) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_annotation_via_ortho_has_alliance_source_type():
    query = "match (gene:Gene)--(deg:Association:DiseaseEntityJoin)--(ec:EvidenceCode) " \
            "where ec.primaryKey = 'IEA'" \
            "and deg.dataProvider = 'Alliance'" \
            "return count(gene) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_via_ortho_exists_for_holoprosencephaly3():
    query = "match (speciesg:Species)--(g:Gene)--(deg:DiseaseEntityJoin)--(do:DOTerm), " \
            "(deg)--(g2:Gene)--(species2:Species) where g.primaryKey='HGNC:10848' " \
            "and do.name = 'holoprosencephaly 3' " \
            "return count(deg) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 6


def test_gene_has_two_ortho_disease_annotations():
    query = "match (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene), (d)--(do:DOTerm) " \
            "where gene.primaryKey = 'MGI:98371' and ortho.primaryKey='HGNC:11204' return count(d) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_human_gene_has_zebrafish_ortho_disease_annotation():
    query = "match (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene), (d)--(do:DOTerm) " \
            "where ortho.primaryKey = 'ZFIN:ZDB-GENE-060312-41' " \
            "and gene.primaryKey='HGNC:12597' return count(d) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_human_gene_has_mouse_ortho_disease_annotation():
    query = "match (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene), (d)--(do:DOTerm) " \
            "where ortho.primaryKey = 'MGI:1919338' " \
            "and gene.primaryKey='HGNC:12597' return count(d) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


