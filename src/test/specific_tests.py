from etl import Neo4jHelper


def execute_transaction(query):
    return Neo4jHelper.run_single_query(query)


def test_fgf8a_exists():
    query = "MATCH (g:Gene) WHERE g.symbol = 'fgf8a' RETURN count(g) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_doterm_exists():
    query = "MATCH(n:DOTerm) where n.primaryKey = 'DOID:0001816' RETURN count(n) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] == 1


def test_isobsolete_false():
    query = "MATCH(n:DOTerm) where n.isObsolete = 'false' RETURN count(n) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_currated_disease_associations_have_date_assigned():
    query = "MATCH (n:DiseaseEntityJoin)--(p:PublicationJoin) WHERE NOT n.joinType IN ['implicated_via_orthology', 'biomarker_via_orthology'] AND NOT EXISTS(p.dateAssigned)" \
            "RETURN COUNT(n) as count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] == 0


def test_species_disease_pub_gene_exists():
    query = "MATCH (s:Species)--(g:Gene)--(dg:DiseaseEntityJoin)--(pubECJ:PublicationJoin)--(p:Publication) " \
            "RETURN COUNT(p) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_species_disease_pub_allele_exists():
    query = "MATCH (s:Species)--(a:Allele:Feature)--(dg:DiseaseEntityJoin)--(pubECJ:PublicationJoin)--(p:Publication) " \
            "RETURN COUNT(p) AS count"
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


def test_mods_have_gene_expression_atlas_link():
    query = "MATCH (g:Gene)-[]-(c:CrossReference) " \
            "WHERE c.crossRefType = 'gene/expression-atlas' " \
            "RETURN count(distinct(g.taxonId)) AS counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_xref_complete_url_is_formatted():
    query = "MATCH (cr:CrossReference) where not cr.crossRefCompleteUrl =~ 'http.*' " \
            "and cr.crossRefType <> 'interaction' " \
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
    query = "MATCH (g:Gene) where g.primaryKey in ['SGD:S000002536'," \
              "'ZFIN:ZDB-GENE-990415-131', 'ZFIN:ZDB-GENE-050517-20', 'FB:FBgn0027655', " \
              "'FB:FBgn0045035','RGD:68337', 'RGD:2332', 'MGI:96067', 'MGI:88388', 'MGI:107202', 'MGI:106658', " \
              "'MGI:105043', 'HGNC:4851', 'HGNC:1884', 'HGNC:795', 'HGNC:11291','RGD:1593265', 'RGD:1559787'] " \
            "and (not (g.automatedGeneSynopsis =~ '.*xhibits.*' " \
              "or g.automatedGeneSynopsis =~ '.*nvolved in.*'or g.automatedGeneSynopsis =~ '.*ocalizes to.*'" \
              "or g.automatedGeneSynopsis =~ '.*redicted to have.*'" \
              "or g.automatedGeneSynopsis =~ '.*redicted to be involved in.*')" \
            "or not (g.automatedGeneSynopsis =~ '.*sed to study.*' " \
              "or g.automatedGeneSynopsis =~ '.*mplicated in.*')) return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 0


def test_nephrogenic_diabetes_insipidus_has_at_least_one_gene():
    query = "MATCH (d:DOTerm)-[]-(g:Gene) where d.name = 'nephrogenic diabetes insipidus' return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_zdb_alt_160129_6_has_at_least_one_disease():
    query = "MATCH (d:DOTerm)-[]-(a:Allele) where a.dataProvider = 'ZFIN' " \
            "and a.primaryKey ='ZFIN:ZDB-ALT-160129-6' return count(a) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_do_terms_have_parents():
    query = "MATCH (d:DOTerm) WHERE NOT (d)-[:IS_A]->() " \
            "and d.primaryKey =~ 'DO:.*'" \
            "and d.isObsolete = 'false' and d.doId <> 'DOID:4' return count(d) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_phenotype_for_all_species_exists():
    query = "MATCH (s:Species)--(r)--(p:Phenotype) " \
            "where labels(r) = ['Gene'] or labels(r) = ['Feature', 'Allele']  RETURN count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 6


def test_variant_for_expected_species_exists():
    query = "MATCH (s:Species)--(r)--(p:Variant) " \
            "where labels(r) = ['Feature', 'Allele'] " \
            "RETURN count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 5


def test_disease_for_all_species_exists():
    query = "MATCH (s:Species)--(r)-[sdot:IS_IMPLICATED_IN|IS_MARKER_FOR]-(dot:DOTerm) " \
            "where labels(r) = ['Gene'] or labels(r) = ['Feature', 'Allele']" \
            "RETURN count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_goannot_for_all_species_exists():
    query = "MATCH (s:Species)--(g:Gene)-[hp:ANNOTATED_TO]-(got:GOTerm) RETURN count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_molint_for_all_species_exists():
    query = "MATCH (s:Species)--(:Gene)--(molint:InteractionGeneJoin) RETURN count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


#def test_variant_consequences_for_five_species_exists():
#    query = "MATCH (s:Species)--(:Gene)--(feature:Feature)--(v:Variant)--(glc:GeneLevelConsequence) RETURN count(distinct s) as counter"
#    result = execute_transaction(query)
#    for record in result:
#        assert record["counter"] == 5


def test_expression_for_non_human_species_exists():
    query = "MATCH (s:Species)--(:Gene)-[hp:EXPRESSED_IN]-(e:ExpressionBioEntity) RETURN count(distinct s) as counter"
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
    query = "MATCH (n:MMOTerm) where n.primaryKey = 'MMO:0000658' and n.displaySynonym = 'RNA in situ'" \
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
    query = "match (gene:Gene)--(deg:Association:DiseaseEntityJoin)--(pubECJ:PublicationJoin)--(ec:ECOTerm) " \
            "where ec.primaryKey = 'ECO:0000501'" \
            "and deg.dataProvider = 'Alliance'" \
            "return count(gene) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_annotation_via_ortho_has_publication():
    query = "match (gene:Gene)--(deg:Association:DiseaseEntityJoin)--(pubECJ:PublicationJoin)--(pub:Publication) " \
            "where" \
            " deg.dataProvider = 'Alliance'" \
            "return count(gene) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_annotation_has_publication():
        query = "match (gene:Gene)--(deg:Association:DiseaseEntityJoin)--(pubECJ:PublicationJoin)--(pub:Publication) " \
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
        assert record["counter"] > 0


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


def test_worm_gene_has_human_alzheimers_via_ortho():
    query = "match (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene), (d)--(do:DOTerm)" \
            "where gene.primaryKey = 'WB:WBGene00000898'" \
            "and do.primaryKey = 'DOID:10652'"  \
            "and ortho.primaryKey = 'HGNC:6091'" \
            "return count(d) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_worm_gene_has_rat_alzheimers_via_ortho():
    query = "match (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene), (d)--(do:DOTerm)" \
            "where gene.primaryKey = 'WB:WBGene00000898'" \
            "and do.primaryKey = 'DOID:10652'" \
            "and ortho.primaryKey = 'RGD:2869'" \
            "return count(d) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_worm_gene2_has_rat_alzheimers_via_ortho():
    query = "match (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene), (d)--(do:DOTerm)" \
            "where gene.primaryKey = 'WB:WBGene00000898'" \
            "and do.primaryKey = 'DOID:10652'" \
            "and ortho.primaryKey = 'RGD:2917'" \
            "return count(d) as counter"
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


def test_human_gene_has_hgnc_cross_reference():
    query = "match (g:Gene)--(cr:CrossReference) where g.primaryKey = 'HGNC:11204'" \
            "and cr.crossRefType = 'gene'" \
            "and cr.globalCrossRefId = 'HGNC:11204'" \
            "and cr.crossRefCompleteUrl = 'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=HGNC:11204'" \
            "return count(cr) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_human_gene_has_rgd_cross_reference():
    query = "match (g:Gene)--(cr:CrossReference) where g.primaryKey = 'HGNC:11204'" \
            "and cr.crossRefType = 'generic_cross_reference'" \
            "and cr.globalCrossRefId = 'RGD:1322513'" \
            "and cr.crossRefCompleteUrl = 'https://rgd.mcw.edu/rgdweb/elasticResults.html?term=1322513'" \
            "return count(cr) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_human_gene_has_rgd_references_cross_reference():
    query = "match (g:Gene)--(cr:CrossReference) where g.primaryKey = 'HGNC:11204'" \
            "and cr.crossRefType = 'gene/references'" \
            "and cr.globalCrossRefId = 'RGD:1322513'" \
            "and cr.crossRefCompleteUrl = 'https://rgd.mcw.edu/rgdweb/report/gene/main.html?view=5&id=1322513'" \
            "return count(cr) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_gene_has_symbol_with_species():
    query = "match (gene:Gene) where gene.symbolWithSpecies = 'fgf8a (Dre)' and gene.symbol = 'fgf8a' " \
            "return count(gene) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_genome_start_is_long():
    query = "match (gene:Gene)-[gf:ASSOCIATION]-(ch:GenomicLocation) where ch.start <> toInt(ch.start) return count(gf) " \
            "as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_genome_end_is_long():
    query = "match (gene:Gene)-[gf:ASSOCIATION]-(ch:GenomicLocation) where ch.end <> toInt(ch.end) " \
            "return count(gf) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_phylogenetic_order_is_int():
    query = "match (g:Species) where g.phylogeneticOrder <> toInt(g.phylogeneticOrder) " \
            "return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_all_species_have_order():
    query = "match (g:Species) where g.phylogeneticOrder is null " \
            "return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_ortho_is_strict_filter_is_boolean():
    query = "match (g1:Gene)-[orth:ORTHOLOGOUS]->(g2:Gene) " \
            "where orth.strictFilter <> toBoolean(orth.strictFilter) " \
            "return count(orth) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_ortho_moderate_filter_is_boolean():
    query = "match (g1:Gene)-[orth:ORTHOLOGOUS]->(g2:Gene) " \
            "where orth.moderateFilter <> toBoolean(orth.moderateFilter) " \
            "return count(orth) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_go_term_has_type_biological_process():
    query = "match (go:GOTerm) where go.primaryKey = 'GO:0000003' and go.type = 'biological_process' " \
            "return count(go) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_sgd_gene_has_gene_disease_ortho():
    query = "match (d:DiseaseEntityJoin)-[:ASSOCIATION]-(g:Gene) where g.primaryKey " \
            "= 'SGD:S000002536' return count(d) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 1


def test_mmo_term_has_display_alias():
    query = "match (mmo:MMOTerm) where mmo.primaryKey " \
            "= 'MMO:0000642' and mmo.displaySynonym = 'protein expression' return count(mmo) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_expression_for_mgi_109583():
    query = "match (g:Gene)--(ebge:BioEntityGeneExpressionJoin)--(e:ExpressionBioEntity)--(o:Ontology) " \
            "where o.name = 'spinal cord' and g.primaryKey = 'MGI:109583' " \
            "return count(distinct ebge) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 2


def test_part_of_relations_exist():
    query = "match (e:EMAPATerm)--(em:EMAPATerm) where e.name = 'nucleus pulposus' " \
            "and em.name = 'intervertebral disc' return count(e) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_expression_images_cross_references_for_species_exists():
    query = "match (s:Species)--(g:Gene)--(cr:CrossReference) where cr.page = 'gene/expression_images' " \
            "return count(distinct s) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 4


def test_eco_term_has_display_synonym():
    query = "match (e:ECOTerm:Ontology) where e.primaryKey = 'ECO:0000269' and e.displaySynonym = 'EXP'" \
            "return count(e) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_point_mutation_hgvs():
    query = "match (a:Allele:Feature)--(v:Variant) where v.hgvsNomenclature = 'NC_007124.7:g.50540171C>T' " \
            "and a.primaryKey='ZFIN:ZDB-ALT-160601-8105' " \
            "return count(v) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_variant_consequence():
    query = "match (a:Allele:Feature)--(v:Variant)--(vc:GeneLevelConsequence) where v.hgvsNomenclature = 'NC_007124.7:g.50540171C>T' " \
            "and a.primaryKey='ZFIN:ZDB-ALT-160601-8105' " \
            "and vc.geneLevelConsequence = 'splice_donor_variant'" \
            "return count(v) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_deletion_hgvs():
    query = "match (a:Allele:Feature)--(v:Variant) where v.hgvsNomenclature = 'NC_007116.7:g.72118557_72118563del' " \
            "and a.primaryKey='ZFIN:ZDB-ALT-170321-11' " \
            "return count(v) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_insertion_hgvs():
    query = "match (a:Allele:Feature)--(v:Variant)--(vc:GeneLevelConsequence) " \
            "where v.hgvsNomenclature = 'NC_007121.7:g.16027812_16027813insCCGTT' " \
            "and a.primaryKey='ZFIN:ZDB-ALT-180207-16' " \
            "return count(v) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_hgnc_gene_has_curated_and_loaded_db_xref():
    query = "match (g:Gene)--(dej:DiseaseEntityJoin)-[:ANNOTATION_SOURCE_CROSS_REFERENCE]-(cr:CrossReference)" \
            " where g.primaryKey = 'HGNC:7' " \
            " return count(cr) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 1


def test_pej_has_agm():
    query = "match (agm:AffectedGenomicModel)-[:PRIMARY_GENETIC_ENTITY]-(pej:PublicationJoin) " \
            "where agm.primaryKey = 'ZFIN:ZDB-FISH-190411-12'" \
            " return count(agm) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_allele_has_description():
    query = "match (a:Allele)--(cr:CrossReference) where cr.crossRefType = 'allele/references' " \
            "return count(a) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_allele_has_submitted_description():
    query = "match (a:Allele) where a.description is not null " \
            "return count(a) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_sgd_gene_has_dej_with_many_orthologous_genes():
    query = "match (dej:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(g:Gene) " \
            "where dej.primaryKey = 'SGD:S000005844DOID:14501IS_IMPLICATED_INHGNC:29567HGNC:3570HGNC:3571HGNC:16526HGNC:16496HGNC:10996HGNC:10998' "\
            "return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_spaw_should_have_disease_genes():
    query = "match (dej:DiseaseEntityJoin)--(g:Gene) " \
            "where g.primaryKey = 'ZFIN:ZDB-GENE-030219-1' "\
            "return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_wb_transgene_has_phenotype():
    query = "match (a:Allele)--(pej:PhenotypeEntityJoin) " \
            "where a.primaryKey = 'WB:WBTransgene00001048' "\
            "return count(a) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_wb_gene_has_inferred_from_allele():
    query = "match (g:Gene)--(dej:DiseaseEntityJoin)--(pej:PublicationJoin)--(a:Allele) " \
            "where g.primaryKey = 'WB:WBGene00000149' " \
            "and a.primaryKey = 'WB:WBVar00275424'" \
            "return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0

# currently WB file is not submitting these, will reactivate when we get a corrected file.
# def test_wb_genes_have_phenotype():
#     query = "match (g:Gene)--(pej:PhenotypeEntityJoin)--(pej:PublicationJoin)--(a:Allele) " \
#             "where g.primaryKey in ['WB:WBGene00000898','WB:WBGene00013817','WB:WBGene00004077']" \
#             "return count(g) as counter"
#     result = execute_transaction(query)
#     for record in result:
#         assert record["counter"] > 2


def test_human_gene_has_disease():
    query = "match (g:Gene)--(dej:DiseaseEntityJoin) " \
            "where g.primaryKey = 'HGNC:11950'" \
            "return count(g) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_mi_term_has_name_flybase():
    query = "match (o:MITerm) " \
            "where o.label = 'FlyBase'" \
            "return count(o) as counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0