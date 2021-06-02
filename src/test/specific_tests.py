from etl import Neo4jHelper


def execute_transaction(query):
    """Excute Transactor"""

    return Neo4jHelper.run_single_query(query)


def test_fgf8a_exists():
    """Test fgf8a Exists"""

    query = """MATCH (g:Gene)
               WHERE g.symbol = 'fgf8a'
               RETURN count(g) AS count"""
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_doterm_exists():
    """Test DO Term Exists"""

    query = """MATCH(n:DOTerm)
               WHERE n.primaryKey = 'DOID:0001816'
               RETURN count(n) AS count"""
    result = execute_transaction(query)
    for record in result:
        assert record["count"] == 1


def test_isobsolete_false():
    """Test isobsolete False"""

    query = """MATCH(n:DOTerm)
               WHERE n.isObsolete = 'false'
               RETURN count(n) AS count"""
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_currated_disease_associations_have_date_assigned():
    """Test Currated Disease Associations Have Date Assigned"""

    query = """MATCH (n:DiseaseEntityJoin)--(p:PublicationJoin)
               WHERE NOT n.joinType IN ['implicated_via_orthology', 'biomarker_via_orthology']
                     AND NOT EXISTS(p.dateAssigned)
               RETURN COUNT(n) AS count"""
    result = execute_transaction(query)
    for record in result:
        assert record["count"] == 0


def test_species_disease_pub_gene_exists():
    """Test Species Disease Pub Gene Exists"""

    query = """
        MATCH (s:Species)--(g:Gene)--(dg:DiseaseEntityJoin)--(pubECJ:PublicationJoin)--(p:Publication)
        RETURN COUNT(p) AS count"""
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_species_disease_pub_allele_exists():
    """Test Species Disease Pub Allele Exists"""

    query = """
        MATCH (s:Species)--(a:Allele:Feature)--(dg:DiseaseEntityJoin)--(pubECJ:PublicationJoin)--(p:Publication)
        RETURN COUNT(p) AS count"""
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_uuid_is_not_duplicated():
    """Test UUID is Not Duplicated"""

    query = """MATCH (g)
               WITH g.uuid AS uuid, count(*)
               AS counter WHERE counter > 0 AND g.uuid IS NOT NULL
               RETURN uuid, counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 2


def test_zfin_gene_has_expression_link():
    """Test ZFIN Gene Has Expression Link"""

    query = """MATCH (g:Gene)-[]-(c:CrossReference)
               WHERE g.primaryKey = 'ZFIN:ZDB-GENE-990415-72'
                     AND c.crossRefType = 'gene/expression'
               RETURN count(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_mods_have_gene_expression_atlas_link():
    """Test MODs have Gene Expression Atlass Links"""

    query = """MATCH (g:Gene)-[]-(c:CrossReference)
               WHERE c.crossRefType = 'gene/expression-atlas'
               RETURN count(distinct(g.taxonId)) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_xref_complete_url_is_formatted():
    """Test XREF Complete URL is Formatted"""

    query = """MATCH (cr:CrossReference) WHERE NOT cr.crossRefCompleteUrl =~ 'http.*'
               RETURN count(cr) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 10


def test_spell_display_name():
    """Test SPELL Display Name"""

    query = """MATCH (cr:CrossReference)
               WHERE cr.prefix = 'SPELL'
                     AND cr.displayName <> 'Serial Patterns of Expression Levels Locator (SPELL)'
               RETURN count(cr) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_spell_cross_ref_type():
    """Test SPELL Cross Ref Type"""

    query = """MATCH (cr:CrossReference)
               WHERE cr.prefix = 'SPELL'
                     AND cr.crossRefType <> 'gene/spell'
               RETURN count(cr) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_genes_have_automated_description():
    """Test Genes Have Automated Description"""

    query = """MATCH (g:Gene) where g.primaryKey IN ['SGD:S000002536', 'FB:FBgn0027655', 'FB:FBgn0045035', 'RGD:68337',
                                                     'RGD:2332', 'MGI:96067', 'MGI:88388', 'MGI:107202', 'MGI:106658',
                                                     'MGI:105043', 'HGNC:4851', 'ZFIN:ZDB-GENE-990415-131',
                                                     'HGNC:1884', 'HGNC:795', 'HGNC:11291','RGD:1593265',
                                                     'RGD:1559787', 'ZFIN:ZDB-GENE-050517-20',
                                                     'ZFIN:ZDB-GENE-990415-131', 'ZFIN:ZDB-GENE-030131-4430']
               AND g.automatedGeneSynopsis IS NULL
               RETURN count(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 0


def test_at_least_one_gene_has_go_description():
    """Test At Least One Gene Has GO Description"""

    query = """MATCH (g:Gene)
               WHERE (g.automatedGeneSynopsis =~ '.*xhibits.*' OR g.automatedGeneSynopsis =~ '.*nvolved in.*'
                      OR g.automatedGeneSynopsis =~ '.*ocalizes to.*'
                      OR g.automatedGeneSynopsis =~ '.*redicted to have.*'
                      OR g.automatedGeneSynopsis =~ '.*redicted to be involved in.*'
                      OR g.automatedGeneSynopsis =~ '.*redicted to localize to.*')
               RETURN COUNT(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_at_least_one_gene_has_disease_description():
    """Test At Least One Gene Has Disease Description"""

    query = """MATCH (g:Gene)
               WHERE (g.automatedGeneSynopsis =~ '.*sed to study.*'
                      OR g.automatedGeneSynopsis =~ '.*mplicated in.*'
                      OR g.automatedGeneSynopsis =~ '.*iomarker of.*')
               RETURN COUNT(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_at_least_one_gene_has_expression_description():
    """Test At Least One Gene Has Expression Description"""

    query = """MATCH (g:Gene)
               WHERE (g.automatedGeneSynopsis =~ '.*s expressed in.*'
                      OR g.automatedGeneSynopsis =~ '.*s enriched in.*')
               RETURN COUNT(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_at_least_one_gene_has_orthology_description():
    """Test At Least One Gene Has Orthology Description"""

    query = """MATCH (g:Gene)
               WHERE g.automatedGeneSynopsis =~ '.*rthologous to.*'
               RETURN COUNT(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_nephrogenic_diabetes_insipidus_has_at_least_one_gene():
    """Test Nephrogenic Diabetes Insipidus Has at Leat One Gene"""

    query = """MATCH (d:DOTerm)-[]-(g:Gene)
               WHERE d.name = 'nephrogenic diabetes insipidus'
               RETURN count(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_zdb_alt_160129_6_has_at_least_one_disease():
    """Test ZDB ALT 160129 6 Has at Lease One Disease"""

    query = """MATCH (d:DOTerm)-[]-(a:Allele)
               WHERE a.dataProvider = 'ZFIN'
                     AND a.primaryKey ='ZFIN:ZDB-ALT-160129-6'
               RETURN count(a) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_do_terms_have_parents():
    """Test DO Terms Have Parents"""

    query = """MATCH (d:DOTerm)
               WHERE NOT (d)-[:IS_A]->()
                     AND d.primaryKey =~ 'DO:.*'
                     AND d.isObsolete = 'false'
                     AND d.doId <> 'DOID:4'
               RETURN count(d) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_phenotype_for_all_species_exists():
    """
    Test Phenotype For Expected Species Exists
    Not used for SARS-CoV-2.
    """

    query = """MATCH (s:Species)--(r)--(p:Phenotype)
               WHERE labels(r) = ['Gene']
                     OR labels(r) = ['Feature', 'Allele']
               RETURN count(distinct s) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_variant_for_expected_species_exists():
    """Test Variant for Expected Species Exists"""

    query = """MATCH (s:Species)--(r)--(p:Variant)
               WHERE labels(r) = ['Feature', 'Allele']
               RETURN count(distinct s) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 5


def test_disease_for_all_species_exists():
    """
    Test Disease for Expected Species Exists
    Not used for SARS-CoV-2.
    """

    query = """MATCH (s:Species)--(r)-[sdot:IS_IMPLICATED_IN|IS_MARKER_FOR]-(dot:DOTerm)
               WHERE labels(r) = ['Gene']
                     OR labels(r) = ['Feature', 'Allele']
               RETURN count(distinct s) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_goannot_for_all_species_exists():
    """
    Test GO Annotation for Expected Species Exists
    Not used for SARS-CoV-2.
    """

    query = """MATCH (s:Species)--(g:Gene)-[hp:ANNOTATED_TO]-(got:GOTerm)
               RETURN count(distinct s) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_molint_for_all_species_exists():
    """Test Molecular Interaction for all Species Exists"""

    query = """MATCH (s:Species)--(:Gene)--(molint:InteractionGeneJoin)
               RETURN count(distinct s) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 8


def test_vepgene_for_all_species_exists():
    """Test Molecular Interaction for all Species Exists

    Current build has:-
    Rno, Mmu, Dre, Cel, Dme
    So make future ones too do.
    """
    species_list = ('Rno', 'Mmu', 'Dre', 'Cel', 'Dme')
    query = """MATCH (s:Species)--(:Gene)--(glc:GeneLevelConsequence)
               RETURN count(distinct s) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 5

    query = """MATCH (s:Species)--(:Gene)--(glc:GeneLevelConsequence)
               RETURN distinct s.species) AS species_abbr"""
    result = execute_transaction(query)
    for record in result:
        assert record["species_abbr"] in species_list


def test_veptranscript_for_all_species_exists():
    """Test Molecular Interaction for all Species Exists"""

    query = """MATCH (s:Species)--(:Gene)--(:Transcript)--(glc:TranscriptLevelConsequence)
               RETURN count(distinct s) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 5


# def test_variant_consequences_for_five_species_exists():
#    """Test Variant Consequences for all Five Species Exists"""
#    query = \
#     """MATCH (s:Species)--(:Gene)--(feature:Feature)--(v:Variant)--(glc:GeneLevelConsequence)
#               RETURN count(distinct s) AS counter"""
#    result = execute_transaction(query)
#    for record in result:
#        assert record["counter"] == 5


def test_expression_for_non_human_species_exists():
    """
    Test Expression for Non Human Species Exists
    Not used for SARS-CoV-2.
    """

    query = """MATCH (s:Species)--(:Gene)-[hp:EXPRESSED_IN]-(e:ExpressionBioEntity)
             RETURN count(distinct s) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 6


def test_cellular_component_relationship_for_expression_exists():
    """Test Cellular Component Relationship For Expression Exists"""

    query = """MATCH (n:ExpressionBioEntity)-[r:CELLULAR_COMPONENT]-(g:GOTerm)
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_structure_relationship_for_expression_exists():
    """Test Anatomical Strucutre Relationship for Expression Exists"""

    query = """MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_STRUCTURE]-(o:Ontology)
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_sub_structure_relationship_for_expression_exists():
    """Test Anatomical Substructure Relationship for Expression Exists"""

    query = """MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_SUB_SUBSTRUCTURE]-(o:Ontology)
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_structure_qualifier_relationship_for_expression_exists():
    """Test Anatomical Structure Qualifier Relationship For Expression Exists"""

    query = """MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_STRUCTURE_QUALIFIER]-(o:Ontology)
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_cellular_component_qualifier_relationship_for_expression_exists():
    """Test Cellular Component Qualifier Relationship For Exprssion Exists"""

    query = """MATCH (n:ExpressionBioEntity)-[r:CELLULAR_COMPONENT_QUALIFIER]-(o:Ontology)
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_sub_structure_qualifier_relationship_for_expression_exists():
    """Test Anaatomical Sub Strucutre qualifier Relationship For Exprssion Exists"""

    query = """MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_SUB_STRUCTURE_QUALIFIER]-(o:Ontology)
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_structure_uberon_relationship_for_expression_exists():
    """Test Anatomical Structure UBERON Relationship for Expression Exists"""

    query = """MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_RIBBON_TERM]-(o:UBERONTerm:Ontology)
               WHERE o.primaryKey <> 'UBERON:AnatomyOtherLocation'
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_anatomical_structure_uberon_other_relationship_for_expression_exists():
    """Test Anatomical Strucutre UBERON Other Relationship for Expression Exists"""

    query = """MATCH (n:ExpressionBioEntity)-[r:ANATOMICAL_RIBBON_TERM]-(o:UBERONTerm:Ontology)
               WHERE o.primaryKey = 'UBERON:AnatomyOtherLocation'
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gocc_other_relationship_for_expression_exists():
    """Test GOCC Other Relationship For Expression Exists"""

    query = """MATCH (n:ExpressionBioEntity)-[r:CELLULAR_COMPONENT_RIBBON_TERM]-(o:GOTerm:Ontology)
               WHERE o.primaryKey = 'GO:otherLocations'
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gocc_ribbon_relationship_for_expression_exists():
    """Test GOCC Ribbon Relationship for Expression Exists"""

    query = """MATCH (n:ExpressionBioEntity)-[r:CELLULAR_COMPONENT_RIBBON_TERM]-(o:GOTerm:Ontology)
               WHERE o.primaryKey <> 'GO:otherLocations'
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_stage_uberon_other_relationship_for_expression_exists():
    """Test Stage UBERON Other Relationship for Expression Exists"""

    query = """MATCH (n:BioEntityGeneExpressionJoin)-[r:STAGE_RIBBON_TERM]-(o:UBERONTerm:Ontology)
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_stage_uberon_relationship_for_expression_exists():
    """Test Stage UBERON Relationship For Expression Exists"""

    query = """MATCH (n:BioEntityGeneExpressionJoin)-[r:STAGE_RIBBON_TERM]-(o:UBERONTerm:Ontology)
               RETURN count(r) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_mmoterm_has_display_synonym():
    """Test MMO Term has Display Synonym"""

    query = """MATCH (n:MMOTerm)
               WHERE n.primaryKey = 'MMO:0000658' AND n.displaySynonym = 'RNA in situ'
               RETURN count(n) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_crip2_has_cardiac_neural_crest():
    """Test crip2 has Cardiac Neural Crest"""

    query = """
    MATCH (gene:Gene)--(ebe:ExpressionBioEntity)--(ei:BioEntityGeneExpressionJoin)--(pub:Publication)
       WHERE ebe.whereExpressedStatement = 'cardiac neural crest'
          AND gene.primaryKey = 'ZFIN:ZDB-GENE-040426-2889'
          AND pub.pubModId = 'ZFIN:ZDB-PUB-130309-4'
       RETURN count(gene) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_expression_gocc_other_term_for_specific_gene_exists():
    """Test Expression GOCC Other Term For Specific Gene Exists"""

    query = """MATCH (g:Gene)--(ebe:ExpressionBioEntity)-[cc:CELLULAR_COMPONENT]-(go:GOTerm),
                     (ebe)-[cr:CELLULAR_COMPONENT_RIBBON_TERM]-(got:GOTerm)
               WHERE  g.primaryKey = 'RGD:2129'
                      AND ebe.whereExpressedStatement = 'vesicle lumen'
                      AND got.primaryKey = 'GO:otherLocations'
               RETURN count(got) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_expression_gocc_term_for_specific_gene_exists():
    """Test Expression GOCC Term For SpecificGene Exists"""

    query = """MATCH (g:Gene)--(ebe:ExpressionBioEntity)-[cc:CELLULAR_COMPONENT]-(go:GOTerm)
               WHERE g.primaryKey = 'RGD:2129'
                     AND ebe.whereExpressedStatement = 'vesicle lumen'
               RETURN count(go) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_gocc_other_has_type():
    """Test GOCC Other Has Type"""

    query = """MATCH (go:GOTerm)
               WHERE go.subset = 'goslim_agr'
                     AND go.type = 'other'
               RETURN count(go) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_gocc_self_ribbon_term_exists():
    """Test GOCC Self Ribbin Term Exists"""

    query = """
    MATCH (gene:Gene)--(ebe:ExpressionBioEntity)-[c:CELLULAR_COMPONENT_RIBBON_TERM]-(got:GOTerm)
    WHERE gene.primaryKey = 'ZFIN:ZDB-GENE-140619-1'
         AND got.primaryKey = 'GO:0005739'
    RETURN count(gene) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_annotation_via_ortho_has_biomarker_relation():
    """Test Gene To Disease Annotation Via Orthology Has Biomarker Relation"""

    query = """MATCH (gene:Gene)-[r:BIOMARKER_VIA_ORTHOLOGY]-(do:DOTerm)
               RETURN count(gene) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_annotation_via_ortho_has_implicated_relation():
    """Test Gene To Disease Annotation Via Orthology has Implicated Relation"""

    query = """MATCH (gene:Gene)-[r:IMPLICATED_VIA_ORTHOLOGY]-(do:DOTerm)
               RETURN count(gene) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_annotation_via_ortho_has_alliance_source_type():
    """Test Gene To Disease Annotation Via ORthology Has Alliance Source Type"""

    query = """
       MATCH (gene:Gene)--(deg:Association:DiseaseEntityJoin)--(pubECJ:PublicationJoin)--(ec:ECOTerm)
       WHERE ec.primaryKey = 'ECO:0000501'
             AND deg.dataProvider = 'Alliance'
       RETURN count(gene) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_annotation_via_ortho_has_publication():
    """Test Gene TO Disease Annotation Via ORthology has Publication"""

    query = """
       MATCH (gene:Gene)--(deg:Association:DiseaseEntityJoin)--(pubECJ:PublicationJoin)--(pub:Publication)
       WHERE deg.dataProvider = 'Alliance'
       RETURN count(gene) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_annotation_has_publication():
    """Test Gene To Disease Annoation Has Publication """

    query = """
        MATCH (gene:Gene)--(deg:Association:DiseaseEntityJoin)--(pubECJ:PublicationJoin)--(pub:Publication)
        RETURN count(gene) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_to_disease_via_ortho_exists_for_holoprosencephaly3():
    """Test Gene To Disease Via Orholoogy Exists For Holoprosencephaly3"""

    query = """MATCH (speciesg:Species)--(g:Gene)--(deg:DiseaseEntityJoin)--(do:DOTerm),
                     (deg)--(g2:Gene)--(species2:Species)
               WHERE g.primaryKey='HGNC:10848'
                   AND do.name = 'holoprosencephaly 3'
               RETURN count(deg) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_has_two_ortho_disease_annotations():
    """Test Gene Has Two Ortho Disease Annotations"""

    query = """
        MATCH (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene),
              (d)--(do:DOTerm)
        WHERE gene.primaryKey = 'MGI:98371'
            AND ortho.primaryKey = 'HGNC:11204'
        RETURN count(d) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_human_gene_has_zebrafish_ortho_disease_annotation():
    """Test Human Gene Has Zebrafish Orhto Diesaes Annotation"""

    query = """MATCH (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene),
                     (d)--(do:DOTerm)
               WHERE ortho.primaryKey = 'ZFIN:ZDB-GENE-060312-41'
                     AND gene.primaryKey = 'HGNC:12597'
               RETURN count(d) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_worm_gene_has_human_alzheimers_via_ortho():
    """Test Worm Gene has Human alzheimers Via Orhto"""

    query = """MATCH (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene),
                     (d)--(do:DOTerm)
               WHERE gene.primaryKey = 'WB:WBGene00000898'
                     AND do.primaryKey = 'DOID:10652'
                     AND ortho.primaryKey = 'HGNC:6091'
               RETURN count(d) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_worm_gene_has_rat_alzheimers_via_ortho():
    """Test Worm Gene has Rat Alzheimers Via Orhtology"""

    query = """MATCH (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene),
                     (d)--(do:DOTerm)
               WHERE gene.primaryKey = 'WB:WBGene00000898'
                     AND do.primaryKey = 'DOID:10652'
                     AND ortho.primaryKey = 'RGD:2869'
               RETURN count(d) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_worm_gene2_has_rat_alzheimers_via_ortho():
    """Test Wrom Gen2 has Rat Alzheimers Via Orthology"""

    query = """MATCH (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene),
                     (d)--(do:DOTerm)
               WHERE gene.primaryKey = 'WB:WBGene00000898'
                     AND do.primaryKey = 'DOID:10652'
                     AND ortho.primaryKey = 'RGD:2917'
               RETURN count(d) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_human_gene_has_mouse_ortho_disease_annotation():
    """Test human Gene has Mouse Ortho Disease Annotation"""

    query = """MATCH (gene:Gene)--(d:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(ortho:Gene),
                     (d)--(do:DOTerm)
               WHERE ortho.primaryKey = 'MGI:1919338'
                     AND gene.primaryKey = 'HGNC:12597'
               RETURN count(d) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_human_gene_has_hgnc_cross_reference():
    """Test Human Gene has HGNC Cross Reference"""

    query = """MATCH (g:Gene)--(cr:CrossReference)
               WHERE g.primaryKey = 'HGNC:11204'
                     AND cr.crossRefType = 'gene'
                     AND cr.globalCrossRefId = 'HGNC:11204'
                     AND cr.crossRefCompleteUrl = 'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=HGNC:11204'
               RETURN count(cr) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_human_gene_has_rgd_cross_reference():
    """Test Human Gene has RGD Cross REference"""

    query = """MATCH (g:Gene)--(cr:CrossReference)
               WHERE g.primaryKey = 'HGNC:11204'
                     AND cr.crossRefType = 'generic_cross_reference'
                     AND cr.globalCrossRefId = 'RGD:1322513'
                     AND cr.crossRefCompleteUrl = 'https://rgd.mcw.edu/rgdweb/elasticResults.html?term=RGD:1322513'
               RETURN count(cr) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_human_gene_has_rgd_references_cross_reference():
    """Test Human Gene has RGD References Cross Reference"""

    query = """MATCH (g:Gene)--(cr:CrossReference)
               WHERE g.primaryKey = 'HGNC:11204'
                     AND cr.crossRefType = 'gene/references'
                     AND cr.globalCrossRefId = 'RGD:1322513'
                     AND cr.crossRefCompleteUrl = 'https://rgd.mcw.edu/rgdweb/report/gene/main.html?view=5&id=1322513'
               RETURN count(cr) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_gene_has_symbol_with_species():
    """Test Gene has Symbol With Species"""

    query = """MATCH (gene:Gene)
               WHERE gene.symbolWithSpecies = 'fgf8a (Dre)'
                     AND gene.symbol = 'fgf8a'
               RETURN count(gene) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_genome_start_is_long():
    """Test Genome Start is Long"""

    query = """MATCH (gene:Gene)-[gf:ASSOCIATION]-(ch:GenomicLocation)
               WHERE ch.start <> toInt(ch.start)
               RETURN count(gf) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_genome_end_is_long():
    """Test Genome End is Long"""

    query = """MATCH (gene:Gene)-[gf:ASSOCIATION]-(ch:GenomicLocation)
               WHERE ch.end <> toInt(ch.end)
               RETURN count(gf) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_phylogenetic_order_is_int():
    """Test PHlogenic Order is Int"""

    query = """MATCH (g:Species)
               WHERE g.phylogeneticOrder <> toInt(g.phylogeneticOrder)
               RETURN count(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_all_species_have_order():
    """Test All Species Hav Order"""

    query = """MATCH (g:Species)
               WHERE g.phylogeneticOrder IS NULL
               RETURN count(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_ortho_is_strict_filter_is_boolean():
    """Test Ortho Is Struct Filter is Boolean"""

    query = """MATCH (g1:Gene)-[orth:ORTHOLOGOUS]->(g2:Gene)
               WHERE orth.strictFilter <> toBoolean(orth.strictFilter)
               RETURN count(orth) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_ortho_moderate_filter_is_boolean():
    """Test Ortho Moderate Filter is Boolean"""

    query = """MATCH (g1:Gene)-[orth:ORTHOLOGOUS]->(g2:Gene)
               WHERE orth.moderateFilter <> toBoolean(orth.moderateFilter)
               RETURN count(orth) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_go_term_has_type_biological_process():
    """Test Go Term has Type Biological Process"""

    query = """MATCH (go:GOTerm)
               WHERE go.primaryKey = 'GO:0000003' AND go.type = 'biological_process'
               RETURN count(go) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_sgd_gene_has_gene_disease_ortho():
    """Test SGD Gene hs Gene Disease Ortho"""

    query = """Match (d:DiseaseEntityJoin)-[:ASSOCIATION]-(g:Gene)
               WHERE g.primaryKey = 'SGD:S000002536'
               RETURN count(d) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 1


def test_mmo_term_has_display_alias():
    """Tst MMO Term hs Display Alias"""

    query = """MATCH (mmo:MMOTerm)
               WHERE mmo.primaryKey = 'MMO:0000642'
                     AND mmo.displaySynonym = 'protein expression'
               RETURN count(mmo) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_expression_for_mgi_109583():
    """Test Expression for MGI 109583"""

    query = """
    MATCH (g:Gene)--(ebge:BioEntityGeneExpressionJoin)--(e:ExpressionBioEntity)--(o:Ontology)
    WHERE o.name = 'spinal cord'
        AND g.primaryKey = 'MGI:109583'
    RETURN count(distinct ebge) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 2


def test_part_of_relations_exist():
    """Test part of Relations Exist"""

    query = """MAtch (e:EMAPATerm)--(em:EMAPATerm)
               WHERE e.name = 'nucleus pulposus'
               AND em.name = 'intervertebral disc'
               RETURN count(e) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_expression_images_cross_references_for_species_exists():
    """Test Expression Images Cross References for Species Exists"""

    query = """MATCH (s:Species)--(g:Gene)--(cr:CrossReference)
               WHERE cr.page = 'gene/expression_images'
               RETURN count(distinct s) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 4


def test_eco_term_has_display_synonym():
    """Test ECO Term has Display Synonym"""

    query = """MATCH (e:ECOTerm:Ontology)
               WHERE e.primaryKey = 'ECO:0000269' AND e.displaySynonym = 'EXP'
               RETURN count(e) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_point_mutation_hgvs():
    """Test Point Mutation HGVS"""

    query = """MATCH (a:Allele:Feature)--(v:Variant)
               WHERE v.primaryKey = 'NC_007124.7:g.50540171C>T'
                     AND a.primaryKey='ZFIN:ZDB-ALT-160601-8105'
               RETURN count(v) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_variant_consequence():
    """Test Variant Consequence"""

    query = """MATCH (a:Allele:Feature)--(v:Variant)--(vc:GeneLevelConsequence)
               WHERE v.primaryKey = 'NC_007124.7:g.50540171C>T'
                     AND a.primaryKey = 'ZFIN:ZDB-ALT-160601-8105'
                     AND vc.geneLevelConsequence = 'splice_donor_variant'
               RETURN count(v) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_deletion_hgvs():
    """Test Deletion HGVS"""

    query = """MATCH (a:Allele:Feature)--(v:Variant)
               WHERE v.primaryKey = 'NC_007116.7:g.72118557_72118563del'
                     AND a.primaryKey='ZFIN:ZDB-ALT-170321-11'
               RETURN count(v) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_insertion_hgvs():
    """Test Insertion HGVS"""

    query = """MATCH (a:Allele:Feature)--(v:Variant)--(vc:GeneLevelConsequence)
               WHERE v.primaryKey = 'NC_007121.7:g.16027812_16027813insCCGTT'
                     AND a.primaryKey = 'ZFIN:ZDB-ALT-180207-16'
               RETURN count(v) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_hgnc_gene_has_curated_and_loaded_db_xref():
    """Test HGNC Gene has Curated and Loaded DB XREF"""

    query = """
    MATCH (g:Gene)--(dej:DiseaseEntityJoin)-[:ANNOTATION_SOURCE_CROSS_REFERENCE]-(cr:CrossReference)
    WHERE g.primaryKey = 'HGNC:7'
    RETURN count(cr) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 1


def test_pej_has_agm():
    """Test PEG has AGM"""

    query = """MATCH (agm:AffectedGenomicModel)-[:PRIMARY_GENETIC_ENTITY]-(pej:PublicationJoin)
               WHERE agm.primaryKey = 'ZFIN:ZDB-FISH-190411-12'
               RETuRN count(agm) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_allele_has_description():
    """Test Allele Has Description"""

    query = """MATCH (a:Allele)--(cr:CrossReference)
               WHERE cr.crossRefType = 'allele/references'
               RETURN count(a) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_allele_has_submitted_description():
    """Test Allele has Submitted Description"""

    query = """MATCH (a:Allele)
               WHERE a.description IS NOT NULL
               RETURN count(a) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_sgd_gene_has_dej_with_many_orthologous_genes():
    """Test SGD Gene has DEJ with Many Ortholous Genes"""

    query = """MATCH (dej:DiseaseEntityJoin)-[:FROM_ORTHOLOGOUS_GENE]-(g:Gene)
               WHERE dej.primaryKey = 'SGD:S000005844IS_IMPLICATED_INDOID:14501HGNC:29567HGNC:3570HGNC:3571HGNC:16526HGNC:16496HGNC:10996HGNC:10998'
               RETURN count(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 7


def test_spaw_should_have_disease_genes():
    """Test Spaw Should have Disease Genes"""

    query = """MATCH (dej:DiseaseEntityJoin)--(g:Gene)
               WHERE g.primaryKey = 'ZFIN:ZDB-GENE-030219-1'
                RETuRN count(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_wb_transgene_has_phenotype():
    """Test WB Transgene has Phenotype"""

    query = """MATCH (a:Allele)--(pej:PhenotypeEntityJoin)
               WHERE a.primaryKey = 'WB:WBTransgene00001048'
               RETURN count(a) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_wb_gene_has_inferred_from_allele():
    """Test WB Gene has Inferred from Allele"""

    query = """MATCH (g:Gene)--(dej:DiseaseEntityJoin)--(pej:PublicationJoin)--(a:Allele)
               WHERE g.primaryKey = 'WB:WBGene00000149'
                     AND a.primaryKey = 'WB:WBVar00275424'
               RETURN count(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0

# currently WB file is not submitting these, will reactivate when we get a corrected file.
# def test_wb_genes_have_phenotype():
#     """Test WB Genes Have Phenotype"""
#
#     query = """MATCH (g:Gene)--(pej:PhenotypeEntityJoin)--(pej:PublicationJoin)--(a:Allele)
#                WHERE g.primaryKey in ['WB:WBGene00000898','WB:WBGene00013817','WB:WBGene00004077']
#                RETURN count(g) AS counter"""
#     result = execute_transaction(query)
#     for record in result:
#         assert record["counter"] > 2


def test_human_gene_has_disease():
    """Test Human Gene has Disease"""

    query = """MATCH (g:Gene)--(dej:DiseaseEntityJoin)
               WHERE g.primaryKey = 'HGNC:11950'
               RETURN count(g) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_mi_term_has_name_flybase():
    """Test MI Term has Name FlyBase"""

    query = """MATCH (o:MITerm)
               WHERE o.label = 'FlyBase'
               RETURN count(o) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_mi_term_has_corrected_url():
    """Test MI Term has Corrected URL"""

    query = """MATCH (o:MITerm)
               WHERE o.primaryKey = 'MI:0465'
                     AND o.url = 'http://dip.doe-mbi.ucla.edu/'
               RETURN count(o) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_rgd_dej_has_rgd_full_url_cross_reference():
    """Test RGD DEJ has RGD full URL Cross Reference"""

    query = """MATCH (g:Gene)--(dej:DiseaseEntityJoin)--(cr:CrossReference)
            WHERE cr.crossRefCompleteUrl = 'https://rgd.mcw.edu/rgdweb/ontology/annot.html?species=Rat&x=1&acc_id=DOID:583#annot'
            RETURN COUNT(cr) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_human_dej_has_omim_full_url_cross_reference():
    """Test Human DEJ has OMIM Full URL Cross Reference"""

    query = """MATCH (g:Gene)--(dej:DiseaseEntityJoin)--(cr:CrossReference)
               WHERE cr.crossRefCompleteUrl = 'https://www.omim.org/entry/605242'
               RETURN count(cr) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_vep_transcript_consequence_has_cdna_start_end_range():
    """Test VEP Transcript Consequence has cDNA start end range"""

    query = """MATCH (v:Variant)--(t:Transcript)--(tc:TranscriptLevelConsequence)
                WHERE v.primaryKey = 'NC_007112.7:g.236854C>A'
                AND t.primaryKey ='ENSEMBL:ENSDART00000003317'
                AND tc.cdnaStartPosition IS NOT NULL
                AND tc.cdsStartPosition IS NOT NULL
                AND tc.aminoAcidReference IS NOT NULL
                and tc.proteinStartPosition IS NOT NULL
                AND tc.aminoAcidVariation IS NOT NULL
                RETURN COUNT(tc) AS counter"""
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0

# please retain this code for testing purposes.
# def test_node_count_is_consistently_growing():
    # this file is generated in node_count_etl and represents the node labels that have fewer
    # nodes in this run of the loader (assuming this isn't a test run), than in the production copy of the datastore
    # as based on the DB-SUMMARY file produced by the file generator.
#    assert os.stat('tmp/labels_with_fewer_nodes.txt').st_size == 0


def test_variant_consequence_has_codon_change():
    """Test Variant Consequence has Codon Change"""

    query = """ MATCH (v:Variant)--(t:Transcript)--(tc:TranscriptLevelConsequence)
                WHERE v.primaryKey = 'NC_007112.7:g.262775T>A'
                AND t.primaryKey = 'ENSEMBL:ENSDART00000111806'
                AND tc.codonChange IS NOT NULL
                RETURN COUNT(tc) AS counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_orphanet_publication_exists():
    """Test Orphanet Publication Exists"""

    query = """ MATCH (g:Gene)--(p:PhenotypeEntityJoin)--(pu:PublicationJoin)--(pr:Publication)
                WHERE g.primaryKey = 'HGNC:869'
                AND pr.pubModId = 'ORPHA:198'
                RETURN COUNT(pr) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_omim_publication_exists():
    """Test OMIM Publication Exists"""

    query = """ MATCH (g:Gene)--(p:PhenotypeEntityJoin)--(pu:PublicationJoin)--(pr:Publication)
                WHERE g.primaryKey = 'HGNC:1958'
                and pr.pubModId = 'OMIM:600513'
                RETURN count(p) AS counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_wb_gene_has_variant_tc_consequence_exists():
    """Test WB gene has variant and transcript level consequences"""

    query = """ MATCH (g:Gene)--(a:Allele)--(v:Variant)--(tc:TranscriptLevelConsequence)
                WHERE g.primaryKey = 'WB:WBGene00022276'
                RETURN count(g) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_zfin_gene_has_variant_tc_consequence_exists():
    """Test WB gene has variant and transcript level consequences"""

    query = """ MATCH (g:Gene)--(a:Allele)--(v:Variant)--(tc:TranscriptLevelConsequence)
                WHERE g.primaryKey = 'ZFIN:ZDB-GENE-030131-9825'
                RETURN count(g) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_mgi_gene_has_variant_tc_consequence_exists():
    """Test WB gene has variant and transcript level consequences"""

    query = """ MATCH (g:Gene)--(a:Allele)--(v:Variant)--(tc:TranscriptLevelConsequence)
                WHERE g.primaryKey = 'MGI:104554'
                RETURN count(g) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_fb_gene_has_variant_tc_consequence_exists():
    """Test WB gene has variant and transcript level consequences"""

    query = """ MATCH (g:Gene)--(a:Allele)--(v:Variant)--(tc:TranscriptLevelConsequence)
                WHERE g.primaryKey = 'FB:FBgn0031209'
                RETURN count(g) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_fb_allele_synonym_exists():
    """Test FB allele has synonyms"""

    query = """ MATCH (a:Allele)--(s:Synonym)
                WHERE a.primaryKey = 'FB:FBal0138114'
                RETURN count(a) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_pseudogenic_transcript_exists():
    """Test pseudogenic transcript exists"""

    query = """ MATCH (t:Transcript)--(so:SOTerm) WHERE so.primaryKey = 'SO:0000516'
                RETURN count(t) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_protein_consequence_exists():
    """Test FB variant has protein start/end """

    query = """ MATCH (v:Variant)--(tlc:TranscriptLevelConsequence)
                    WHERE v.primaryKey = 'NT_033779.5:g.5464013C>T'
                    AND tlc.proteinRange IS NOT NULL
                    AND tlc.proteinStartPosition IS NOT NULL
                    AND tlc.proteinEndPosition IS NOT NULL
                RETURN count(v) AS counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_codon_consequence_exists():
    """Test FB variant has codon start/end """

    query = """ MATCH (v:Variant)--(tlc:TranscriptLevelConsequence)
                    WHERE v.primaryKey = 'NT_033779.5:g.5464013C>T'
                    AND tlc.codonChange IS NOT NULL
                    AND tlc.codonReference IS NOT NULL
                    AND tlc.codonVariation IS NOT NULL
                RETURN count(v) AS counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_level_sift_results_exist():
    """Test gene level SIFT scores and predictions exist"""

    query = """ MATCH (v:Variant)-[:ASSOCIATION]->(glc:GeneLevelConsequence)
                    WHERE glc.siftPrediction IS NOT NULL
                    AND (
                        glc.siftScore = ''
                        OR (
                            tofloat(glc.siftScore) >=0
                            AND tofloat(glc.siftScore) <= 1
                        )
                    )
                RETURN count(v) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_gene_level_polyphen_results_exist():
    """Test gene level PolyPhen scores and predictions exist"""

    query = """ MATCH (v:Variant)-[:ASSOCIATION]->(glc:GeneLevelConsequence)
                    WHERE glc.polyphenPrediction IS NOT NULL
                    AND (
                        glc.polyphenScore = ''
                        OR (
                            tofloat(glc.polyphenScore) >=0
                            AND tofloat(glc.polyphenScore) <= 1
                        )
                    )
                RETURN count(v) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_transcript_level_sift_results_exist():
    """Test transcript level SIFT scores and predictions exist"""

    query = """ MATCH (v:Variant)-[:ASSOCIATION]->(tlc:TranscriptLevelConsequence)
                    WHERE tlc.siftPrediction IS NOT NULL
                    AND (
                        tlc.siftScore = ''
                        OR (
                            tofloat(tlc.siftScore) >=0
                            AND tofloat(tlc.siftScore) <= 1
                        )
                    )
                RETURN count(v) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_transcript_level_polyphen_results_exist():
    """Test transcript level PolyPhen scores and predictions exist"""

    query = """ MATCH (v:Variant)-[:ASSOCIATION]->(tlc:TranscriptLevelConsequence)
                    WHERE tlc.polyphenPrediction IS NOT NULL
                    AND (
                        tlc.polyphenScore = ''
                        OR (
                            tofloat(tlc.polyphenScore) >=0
                            AND tofloat(tlc.polyphenScore) <= 1
                        )
                    )
                RETURN count(v) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_tc_consequence_is_null_vs_dash():
    """Test FB variant has codon start/end """

    query = """ MATCH (v:Variant)--(tlc:TranscriptLevelConsequence)
                    WHERE v.primaryKey = 'NC_007116.7:g.65946401_65951486delins'
                    AND tlc.aminoAcidVariation <> '-'
                    AND tlc.aminoAcidChange <> '-'
                    AND tlc.aminoAcidReference <> '-'
                RETURN count(v) AS counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_manual_sars2_synonym_exists():
    """Test_manual_sars2_synonym_exists"""

    query = """ MATCH (s:Synonym) WHERE s.name = 'SARS-CoV-2 infection'
                RETURN count(s) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_not_disease_annotation_exists_exists():
    """Test_not_disease_annotation_exists_exists"""

    query = """ MATCH (d:DOTerm)-[x:IS_NOT_MARKER_FOR]-(g:Gene)
                RETURN count(d) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_protein_sequence_exists():
    """Test_protein_sequence_exists"""

    query = """  MATCH (t:Transcript)--(n:TranscriptProteinSequence)
                 WHERE n.proteinSequence IS NOT NULL
                 and n.proteinSequence <> ''
                 RETURN count(distinct t.dataProvider) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 4


def test_fb_variant_has_note():
    """Test variant has note"""

    query = """ MATCH (v:Variant)-[x:ASSOCIATION]-(n:Note)
                WHERE v.primaryKey = 'NT_033777.3:g.31883471_31883472ins'
                RETURN count(v) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_fb_variant_has_note_with_pub():
    """Test variant has note"""

    query = """ MATCH (v:Variant)-[x:ASSOCIATION]-(n:Note)--(p:Publication)
                WHERE v.primaryKey = 'NT_033777.3:g.31883471_31883472ins'
                RETURN count(v) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_correct_number_of_species_have_variant_transcript_exon_relations():
    """Test correct number of species have variant-transcript-exon relations"""

    query = """ MATCH (e:Exon)--(t:Transcript)--(v:Variant)
                RETURN COUNT(DISTINCT t.dataProvider) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 5


def test_correct_number_of_species_datasetsample_relations():
    """test_correct_number_of_species_datasetsample_relations"""

    query = """ MATCH (hd:HTPDataset)--(hds:HTPDatasetSample)
                RETURN COUNT(DISTINCT hd.dataProvider) as counter
    """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 4


def test_correct_number_of_species_phenotype_xrefs_relations():
    """test_correct_number_of_species_phenotype_xrefs_relations"""

    query = """
            MATCH (g:Gene)--(cr:CrossReference)
            WHERE cr.crossRefType = 'gene/phenotypes'
            RETURN count(DISTINCT g.dataProvider) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 4


def test_htp_dataset_has_correct_number_of_preferred_xrefs_relations():
    """test_htp_dataset_has_correct_number_of_preferred_xrefs_relations"""

    query = """
            MATCH (g:HTPDataset)--(cr:CrossReference)
            WHERE cr.crossRefType = 'htp/dataset'
            AND cr.preferred = 'true'
            AND cr.globalCrossRefId = 'SGD:GSE3431'
            RETURN count(DISTINCT cr) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_htp_dataset_has_correct_number_of_not_preferred_xrefs_relations():
    """test_htp_dataset_has_correct_number_of_not_preferred_xrefs_relations"""

    query = """
            MATCH (g:HTPDataset)--(cr:CrossReference)
            WHERE cr.crossRefType = 'htp/dataset'
            AND cr.preferred = 'false'
            AND g.primaryKey = 'GEO:GSE3431'
            and cr.globalCrossRefId = 'GEO:GSE3431'
            RETURN count(DISTINCT cr) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_mgi_reference_url_creation():
    """test_mgi_reference_url_creation"""

    query = """
            MATCH (a:Allele)--(v:Variant)--(p:Publication)
            where a.primaryKey = 'MGI:5806340'
            and p.pubModUrl = 'http://www.informatics.jax.org/reference/MGI:5806759'
            RETURN count(DISTINCT v) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_iagp_name_exists():
    """test_IAGP_name_exists"""

    query = """
            MATCH (e:ECOTerm) where e.displaySynonym = 'IAGP'
            RETURN count(DISTINCT e) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_display_name_for_impc_is_correct():
    """test_display_name_for_impc_is_correct"""

    query = """
            MATCH (cr:CrossReference) where cr.displayName = 'IMPC'
            RETURN count(DISTINCT cr) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_htp_xref_has_preferred_attribute_true_is_correct():
    """test_htp_xref_has_preferred_attribute_true_is_correct"""

    query = """
            MATCH (cr:CrossReference)--(htp:HTPDataset)
            WHERE htp.primaryKey = 'ArrayExpress:E-GEOD-56866'
            AND cr.globalCrossRefId = 'MGI:E-GEOD-56866'
            AND cr.preferred = 'true'
            RETURN count(DISTINCT cr) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_genome_location_for_exon_has_strand():
    """test_genome_location_for_exon_has_strand"""

    query = """
            MATCH (e:Exon)--(gl:GenomicLocation)
            WHERE not exists (gl.strand)
            RETURN count(DISTINCT e) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_genome_location_for_transcript_has_strand():
    """test_genome_location_for_transcript_has_strand"""

    query = """
            MATCH (t:Transcript)--(gl:GenomicLocation)
            WHERE not exists (gl.strand)
            RETURN count(DISTINCT t) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_all_pheno_xrefs_have_display_names():
    """test_all_pheno_xrefs_have_display_names"""

    query = """
            MATCH (t:Gene)--(cr:CrossReference)
            WHERE (cr.displayName = '' or cr.displayName IS NULL)
            AND cr.crossRefType = 'gene/phenotypes'
            RETURN count(DISTINCT cr) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_papers_have_urls():
    """test_papers_have_urls"""

    query = """
            MATCH (p:Publication)
            WHERE (p.pubModUrl IS NULL or p.pubModUrl = '')
            AND p.pubModId is not null
            AND p.pubModId <> ''
            RETURN count(DISTINCT p) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_papers_have_mod_urls():
    """test_papers_have_mod_urls"""

    query = """
            MATCH (p:Publication)--(n)
            WHERE (p.pubModUrl IS NULL or p.pubModUrl = '')
            AND p.pubModId is not null
            AND p.pubModId <> ''
            RETURN count(DISTINCT labels(n)) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1


def test_wb_has_agm():
    """test_wb_has_agm"""

    query = """
            MATCH (a:AffectedGenomicModel)
            WHERE a.primaryKey = 'WB:WBStrain00023353'
            RETURN count(a) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] == 1


def test_wb_has_agm_with_disease():
    """test_wb_has_agm_with_disease"""

    query = """
            MATCH (a:AffectedGenomicModel)--(dej:DiseaseEntityJoin)
            WHERE a.primaryKey = 'WB:WBGenotype00000021'
            RETURN count(a) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def test_fb_variant_has_a_single_note():
    """test_fb_variant_has_a_single_note"""

    query = """
            MATCH (a:Variant)--(n:Note)
            WHERE a.hgvsNomenclature = 'NT_033779.5:g.8419681_8431132del'
            RETURN count(n) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 2


def test_ontology_metadata():
    """Test we have some ontology meta data."""
    query = """
            MATCH (node:OntologyFileMetadata)
            RETURN count(node) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 1


def test_mod_release_metadata():
    """Test we have some mod meta data."""
    query = """
            MATCH (node:ModFileMetadata)
            RETURN count(node) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 1


def test_alliance_release_metadata():
    """Test we have some aliiance release data."""
    query = """
            MATCH (node:AllianceReleaseInfo)
            RETURN count(node) as counter """
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] >= 1


def test_correct_model_experimental_condition_parsing():
    """test correct model experimental condition parsing (ZFIN example)"""
    query = """
            MATCH (d :DOTerm:Ontology {primaryKey: "DOID:9452"})-[:ASSOCIATION]-(dfa :DiseaseEntityJoin {primaryKey: "ZFIN:ZDB-FISH-150901-27842ZECO:0000119ZECO:0000122IS_MODEL_OFDOID:9452"}),
                  (dfa)-[:ASSOCIATION]-(agm :AffectedGenomicModel {primaryKey: "ZFIN:ZDB-FISH-150901-27842"}),
                  (dfa)--(ec:ExperimentalCondition),
                  (dfa)-[:EVIDENCE]-(pubj:PublicationJoin) RETURN DISTINCT COUNT(DISTINCT ec) as ec_count, COUNT(DISTINCT pubj) as pubj_count;"""
    result = execute_transaction(query)
    for record in result:
        assert record["ec_count"] == 2
        assert record["pubj_count"] == 1
