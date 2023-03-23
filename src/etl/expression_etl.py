"""Expression ETL."""

import logging
import codecs
import uuid
import multiprocessing
import ijson

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor


class ExpressionETL(ETL):
    """Expression ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    xrefs_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (o:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
            """ + ETLHelper.get_cypher_xref_text() + """
            }
        IN TRANSACTIONS of %s ROWS"""

    bio_entity_expression_query_template = """

        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MERGE (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                ON CREATE SET e.whereExpressedStatement = row.whereExpressedStatement
            }
        IN TRANSACTIONS of %s ROWS"""

    bio_entity_gene_expression_join_query_template = """

        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (assay:MMOTerm {primaryKey:row.assay})
                MERGE (gej:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                    ON CREATE SET gej.joinType = 'expression',
                    gej:Association

                MERGE (gej)-[geja:ASSAY]->(assay)
            }
        IN TRANSACTIONS of %s ROWS"""

    bio_entity_gene_ao_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (g:Gene {primaryKey:row.geneId})
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otast)
                    AND NOT 'FBCVTerm' in LABELS(otast)

                MERGE (g)-[gex:EXPRESSED_IN]->(e)
                        ON CREATE SET gex.uuid = row.ei_uuid
                MERGE (e)-[gejotast:ANATOMICAL_STRUCTURE]->(otast)
            }
        IN TRANSACTIONS of %s ROWS"""

    add_pubs_query_template = """

        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (gej:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})

                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                        ON CREATE SET pubf.pubModId = row.pubModId,
                        pubf.pubMedId = row.pubMedId,
                        pubf.pubModUrl = row.pubModUrl,
                        pubf.pubMedUrl = row.pubMedUrl

                CREATE (gej)-[gejpubf:EVIDENCE]->(pubf)
            }
        IN TRANSACTIONS of %s ROWS"""

    ao_expression_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                // GET PRIMARY DATA OBJECTS

                // LOAD NODES
                MATCH (g:Gene {primaryKey:row.geneId})
                MATCH (gej:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})

                MERGE (g)-[ggej:ASSOCIATION]->(gej)
                MERGE (e)-[egej:ASSOCIATION]->(gej)
            }
        IN TRANSACTIONS of %s ROWS"""

    sgd_cc_expression_query_template = """

        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                // GET PRIMARY DATA OBJECTS

                // LOAD NODES
                MATCH (g:Gene {primaryKey:row.geneId})
                MATCH (assay:MMOTerm {primaryKey:row.assay})
                MATCH (otcct:GOTerm {primaryKey:row.cellularComponentTermId})

                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MATCH (gej:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                    ON CREATE SET gej :Association

                    MERGE (g)-[gex:EXPRESSED_IN]->(e)
                        ON CREATE SET gex.uuid = row.ei_uuid
                    MERGE (gej)-[geja:ASSAY]->(assay)

                    MERGE (g)-[ggej:ASSOCIATION]->(gej)

                    MERGE (e)-[egej:ASSOCIATION]->(gej)

                    MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)
            }
        IN TRANSACTIONS of %s ROWS"""

    cc_expression_query_template = """

        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                // GET PRIMARY DATA OBJECTS

                // LOAD NODES
                MATCH (g:Gene {primaryKey:row.geneId})
                MATCH (assay:MMOTerm {primaryKey:row.assay})
                MATCH (otcct:GOTerm {primaryKey:row.cellularComponentTermId})

                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MATCH (gej:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})

                    MERGE (g)-[gex:EXPRESSED_IN]->(e)
                        ON CREATE SET gex.uuid = row.ei_uuid


                    MERGE (gej)-[geja:ASSAY]->(assay)

                    MERGE (g)-[ggej:ASSOCIATION]->(gej)

                    MERGE (e)-[egej:ASSOCIATION]->(gej)

                    MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)
            }
        IN TRANSACTIONS of %s ROWS"""

    ao_cc_expression_query_template = """

        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                // GET PRIMARY DATA OBJECTS

                // LOAD NODES
                MATCH (g:Gene {primaryKey:row.geneId})
                MATCH (assay:MMOTerm {primaryKey:row.assay})
                MATCH (otcct:GOTerm {primaryKey:row.cellularComponentTermId})
                MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otast)
                        AND NOT 'FBCVTerm' in LABELS(otast)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MATCH (gej:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})

                WITH g, e, gej, assay, otcct, otast, row WHERE NOT otast IS NULL AND NOT otcct IS NULL


                    MERGE (g)-[gex:EXPRESSED_IN]->(e)
                        ON CREATE SET gex.uuid = row.ei_uuid


                    MERGE (gej)-[geja:ASSAY]->(assay)

                    MERGE (g)-[ggej:ASSOCIATION]->(gej)

                    MERGE (e)-[egej:ASSOCIATION]->(gej)


                    MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)

                    MERGE (e)-[gejotast:ANATOMICAL_STRUCTURE]-(otast)
            }
        IN TRANSACTIONS of %s ROWS"""

    eas_substructure_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otasst)
                        AND NOT 'FBCVTerm' in LABELS(otasst)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MERGE (e)-[eotasst:ANATOMICAL_SUB_SUBSTRUCTURE]->(otasst)
            }
        IN TRANSACTIONS of %s ROWS"""

    eas_qualified_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otastq)
                        AND NOT 'FBCVTerm' in LABELS(otastq)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MERGE (e)-[eotastq:ANATOMICAL_STRUCTURE_QUALIFIER]-(otastq)
            }
        IN TRANSACTIONS of %s ROWS"""

    eass_qualified_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otasstq:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otasstq)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})

                MERGE (e)-[eotasstq:ANATOMICAL_SUB_STRUCTURE_QUALIFIER]-(otasstq)
            }
        IN TRANSACTIONS of %s ROWS"""

    ccq_expression_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otcctq)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})

                MERGE (e)-[eotcctq:CELLULAR_COMPONENT_QUALIFIER]-(otcctq)
            }
        IN TRANSACTIONS of %s ROWS"""

    stage_expression_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                MERGE (s:Stage {primaryKey:row.stageName})
                    ON CREATE SET s.name = row.stageName
                MERGE (ei)-[eotcctq:DURING]-(s)
            }
        IN TRANSACTIONS of %s ROWS"""
    
    uberon_ao_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MATCH (o:UBERONTerm {primaryKey:row.aoUberonId})
                MERGE (ebe)-[ebeo:ANATOMICAL_RIBBON_TERM]-(o)
            }
        IN TRANSACTIONS of %s ROWS"""

    uberon_stage_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                MATCH (o:UBERONTerm {primaryKey:row.uberonStageId})

                MERGE (ei)-[eio:STAGE_RIBBON_TERM]-(o)
            }
        IN TRANSACTIONS of %s ROWS"""

    uberon_ao_other_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MATCH (u:Ontology:UBERONTerm {primaryKey:'UBERON:AnatomyOtherLocation'})
                MERGE (ebe)-[ebeu:ANATOMICAL_RIBBON_TERM]-(u)
            }
        IN TRANSACTIONS of %s ROWS"""

    uberon_stage_other_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                MATCH (u:Ontology:UBERONTerm {primaryKey:'UBERON:PostEmbryonicPreAdult'})

                MERGE (ei)-[eiu:STAGE_RIBBON_TERM]-(u)
            }
        IN TRANSACTIONS of %s ROWS"""

    def __init__(self, config):
        """Ibnitialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        # add the 'other' nodes to support the expression ribbon components.
        self.add_other()

        thread_pool = []
        query_tracking_list = multiprocessing.Manager().list()
        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type,
                                              args=(sub_type, query_tracking_list))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

        queries = []
        for item in query_tracking_list:
            queries.append(item)

        Neo4jTransactor.execute_query_batch(queries)

    def _process_sub_type(self, sub_type, query_tracking_list):

        self.logger.info("Loading Expression Data: %s", sub_type.get_data_provider())
        data_file = sub_type.get_filepath()
        data_provider = sub_type.get_data_provider()

        if data_file is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
            return

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [self.bio_entity_expression_query_template,
             "expression_entities_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.bio_entity_gene_ao_query_template,
             "expression_gene_ao_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.bio_entity_gene_expression_join_query_template,
             "expression_entity_joins_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.ao_expression_query_template,
             "expression_ao_expression_" + sub_type.get_data_provider() + ".csv", commit_size]
        ]

        if data_provider == 'SGD':
            query_template_list += [[self.sgd_cc_expression_query_template,
                                     "expression_SGD_cc_expression_" + sub_type.get_data_provider() + ".csv", commit_size]]
        else:
            query_template_list += [[self.cc_expression_query_template, 
                                     "expression_cc_expression_" + sub_type.get_data_provider() + ".csv", commit_size,]]

        query_template_list += [
            [self.ao_cc_expression_query_template, "expression_ao_cc_expression_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.eas_qualified_query_template, "expression_eas_qualified_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.eas_substructure_query_template, "expression_eas_substructure_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.eass_qualified_query_template, "expression_eass_qualified_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.ccq_expression_query_template, "expression_ccq_expression_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.stage_expression_query_template, "expression_stage_expression_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.uberon_stage_query_template, "expression_uberon_stage_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.uberon_ao_query_template, "expression_uberon_ao_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.uberon_ao_other_query_template, "expression_uberon_ao_other_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.uberon_stage_other_query_template, "expression_uberon_stage_other_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.xrefs_query_template, "expression_cross_references_" + sub_type.get_data_provider() + ".csv", commit_size],
            [self.add_pubs_query_template, "expression_add_pubs_" + sub_type.get_data_provider() + ".csv", commit_size]
        ]

        # Obtain the generator
        generators = self.get_generators(data_file, batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)

        for item in query_and_file_list:
            query_tracking_list.append(item)
        self.error_messages("Expression-{}: ".format(sub_type.get_data_provider()))
        self.logger.info("Finished Loading Expression Data: %s", sub_type.get_data_provider())

    def add_other(self):
        """Add Other."""
        self.logger.debug("made it to the addOther statement")

        add_other_query = """

            MERGE(other:UBERONTerm:Ontology {primaryKey:'UBERON:AnatomyOtherLocation'})
                ON CREATE SET other.name = 'other'
            MERGE(otherstage:UBERONTerm:Ontology {primaryKey:'UBERON:PostEmbryonicPreAdult'})
                ON CREATE SET otherstage.name = 'post embryonic, pre-adult'
            MERGE(othergo:GOTerm:Ontology {primaryKey:'GO:otherLocations'})
                ON CREATE SET othergo.name = 'other locations'
                ON CREATE SET othergo.definition = 'temporary node to group expression entities up to ribbon terms'
                ON CREATE SET othergo.type = 'other'
                ON CREATE SET othergo.subset = 'goslim_agr' """

        Neo4jHelper.run_single_query_no_return(add_other_query)

    def get_generators(self, expression_file, batch_size):  # noqa
        """Get Generators."""

        self.logger.debug("made it to the expression generator")

        counter = 0

        cross_references = []
        bio_entities = []
        bio_join_entities = []
        bio_entity_gene_aos = []
        pubs = []
        ao_expressions = []
        cc_expressions = []
        ao_qualifiers = []
        ao_substructures = []
        ao_ss_qualifiers = []
        cc_qualifiers = []
        ao_cc_expressions = []
        stage_list = []
        stage_uberon_data = []
        uberon_ao_data = []
        uberon_ao_other_data = []
        uberon_stage_other_data = []

        self.logger.debug("streaming json data from %s ...", expression_file)
        with codecs.open(expression_file, 'r', 'utf-8') as file_handle:
            for xpat in ijson.items(file_handle, 'data.item'):
                counter = counter + 1

                pub_med_url = None
                pub_mod_url = None
                pub_med_id = ""
                publication_mod_id = ""
                stage_term_id = ""
                stage_name = ""
                stage_uberon_term_id = ""
                gene_id = xpat.get('geneId')

                if self.test_object.using_test_data() is True:
                    is_it_test_entry = self.test_object.check_for_test_id_entry(gene_id)
                    if is_it_test_entry is False:
                        counter = counter - 1
                        continue

                evidence = xpat.get('evidence')

                if 'publicationId' in evidence:
                    if evidence.get('publicationId').startswith('PMID:'):
                        pub_med_id = evidence.get('publicationId')
                        local_pub_med_id = pub_med_id.split(":")[1]
                        pub_med_prefix = pub_med_id.split(":")[0]
                        pub_med_url = self.etlh.get_no_page_complete_url(
                            local_pub_med_id, pub_med_prefix, gene_id)
                        if pub_med_id is None:
                            pub_med_id = ""

                        if 'crossReference' in evidence:
                            pub_xref = evidence.get('crossReference')
                            publication_mod_id = pub_xref.get('id')

                            if publication_mod_id is not None:
                                pub_mod_url = self.etlh.get_expression_pub_annotation_xref(publication_mod_id)

                    else:
                        publication_mod_id = evidence['publicationId']
                        if publication_mod_id is not None:
                            pub_mod_url = self.etlh.get_expression_pub_annotation_xref(publication_mod_id)

                    if publication_mod_id is None:
                        publication_mod_id = ""

                assay = xpat.get('assay')

                if 'whereExpressed' in xpat:

                    where_expressed = xpat.get('whereExpressed')
                    cellular_component_qualifier_term_id = where_expressed.get('cellularComponentQualifierTermId')
                    cellular_component_term_id = where_expressed.get('cellularComponentTermId')
                    anatomical_structure_term_id = where_expressed.get('anatomicalStructureTermId')
                    anatomical_structure_qualifier_term_id = where_expressed.get(
                        'anatomicalStructureQualifierTermId')
                    anatomical_sub_structure_term_id = where_expressed.get('anatomicalSubStructureTermId')
                    anatomical_sub_structure_qualifier_term_id = where_expressed.get(
                        'anatomicalSubStructureQualifierTermId')
                    where_expressed_statement = where_expressed.get('whereExpressedStatement')

                    when_expressed_stage = xpat.get('whenExpressed')

                    if 'stageTermId' in when_expressed_stage:
                        stage_term_id = when_expressed_stage.get('stageTermId')
                    if 'stageName' in when_expressed_stage:
                        stage_name = when_expressed_stage.get('stageName')

                    # TODO: making unique BioEntityGeneExpressionJoin nodes
                    # and ExpressionBioEntity nodes is tedious.
                    # TODO: Lets get the DQMs to fix this.
                    expression_unique_key = gene_id + assay + stage_name
                    expression_entity_unique_key = ""

                    if anatomical_structure_term_id is not None:
                        expression_unique_key += anatomical_structure_term_id
                        expression_entity_unique_key = anatomical_structure_term_id

                        if anatomical_structure_qualifier_term_id is not None:
                            expression_unique_key += anatomical_structure_qualifier_term_id
                            expression_entity_unique_key += anatomical_structure_qualifier_term_id

                    if cellular_component_term_id is not None:
                        expression_unique_key += cellular_component_term_id
                        expression_entity_unique_key += cellular_component_term_id

                        if cellular_component_qualifier_term_id is not None:
                            expression_unique_key += cellular_component_qualifier_term_id
                            expression_entity_unique_key += cellular_component_qualifier_term_id

                    if anatomical_sub_structure_term_id is not None:
                        expression_unique_key += anatomical_sub_structure_term_id

                        if anatomical_sub_structure_qualifier_term_id is not None:
                            expression_unique_key += anatomical_sub_structure_qualifier_term_id
                            expression_entity_unique_key += anatomical_sub_structure_qualifier_term_id

                    expression_entity_unique_key += where_expressed_statement
                    expression_unique_key += where_expressed_statement

                    if where_expressed.get('anatomicalStructureUberonSlimTermIds') is not None:
                        for uberon_structure_term_object in \
                                where_expressed.get('anatomicalStructureUberonSlimTermIds'):
                            structure_uberon_term_id = \
                                    uberon_structure_term_object.get('uberonTerm')
                            if structure_uberon_term_id is not None \
                                    and structure_uberon_term_id != 'Other':
                                structure_uberon_term = {
                                    "ebe_uuid": expression_entity_unique_key,
                                    "aoUberonId": structure_uberon_term_id}
                                uberon_ao_data.append(structure_uberon_term)
                            elif structure_uberon_term_id is not None \
                                    and structure_uberon_term_id == 'Other':
                                other_structure_uberon_term = {
                                    "ebe_uuid": expression_entity_unique_key}
                                uberon_ao_other_data.append(other_structure_uberon_term)

                    if where_expressed.get('anatomicalSubStructureUberonSlimTermIds') is not None:
                        for uberon_sub_structure_term_object in \
                                where_expressed.get('anatomicalSubStructureUberonSlimTermIds'):
                            sub_structure_uberon_term_id = \
                                    uberon_sub_structure_term_object.get('uberonTerm')
                            if sub_structure_uberon_term_id is not None \
                                    and sub_structure_uberon_term_id != 'Other':
                                sub_structure_uberon_term = {
                                    "ebe_uuid": expression_entity_unique_key,
                                    "aoUberonId": sub_structure_uberon_term_id}
                                uberon_ao_data.append(sub_structure_uberon_term)
                            elif sub_structure_uberon_term_id is not None \
                                    and sub_structure_uberon_term_id == 'Other':
                                other_structure_uberon_term = {
                                    "ebe_uuid": expression_entity_unique_key}
                                uberon_ao_other_data.append(other_structure_uberon_term)

                    if cellular_component_term_id is None:
                        cellular_component_term_id = ""

                    if when_expressed_stage.get('stageUberonSlimTerm') is not None:
                        stage_uberon_term_object = when_expressed_stage.get('stageUberonSlimTerm')
                        stage_uberon_term_id = stage_uberon_term_object.get("uberonTerm")
                        if stage_uberon_term_id is not None and stage_uberon_term_id != "post embryonic, pre-adult":
                            stage_uberon = {
                                "uberonStageId": stage_uberon_term_id,
                                "ei_uuid": expression_unique_key
                            }
                            stage_uberon_data.append(stage_uberon)
                        if stage_uberon_term_id == "post embryonic, pre-adult":
                            stage_uberon_other = {
                                "ei_uuid": expression_unique_key
                            }
                            uberon_stage_other_data.append(stage_uberon_other)

                    if stage_term_id is None or stage_name == 'N/A':
                        stage_term_id = ""
                        stage_name = ""
                        stage_uberon_term_id = ""

                    if stage_name is not None:
                        stage = {
                            "stageTermId": stage_term_id,
                            "stageName": stage_name,
                            "ei_uuid": expression_unique_key}
                        stage_list.append(stage)
                    else:
                        stage_uberon_term_id = ""

                    if 'crossReference' in xpat:
                        cross_ref = xpat.get('crossReference')
                        cross_ref_id = cross_ref.get('id')
                        local_cross_ref_id = cross_ref_id.split(":")[1]
                        prefix = cross_ref.get('id').split(":")[0]
                        pages = cross_ref.get('pages')

                        # some pages collection have 0 elements
                        if pages is not None and len(pages) > 0:
                            for page in pages:
                                if page == 'gene/expression/annotation/detail':
                                    mod_global_cross_ref_id = self.etlh.rdh2.return_url_from_key_value(
                                        prefix, local_cross_ref_id, page)

                                    xref = ETLHelper.get_xref_dict(local_cross_ref_id,
                                                                   prefix,
                                                                   page,
                                                                   page,
                                                                   cross_ref_id,
                                                                   mod_global_cross_ref_id,
                                                                   cross_ref_id + page)
                                    xref['ei_uuid'] = expression_unique_key
                                    cross_references.append(xref)

                    bio_entity = {
                        "ebe_uuid": expression_entity_unique_key,
                        "whereExpressedStatement": where_expressed_statement}
                    bio_entities.append(bio_entity)

                    bio_join_entity = {
                        "ei_uuid": expression_unique_key,
                        "assay": assay}
                    bio_join_entities.append(bio_join_entity)

                    bio_entity_gene_ao = {
                        "geneId": gene_id,
                        "ebe_uuid": expression_entity_unique_key,
                        "anatomicalStructureTermId": anatomical_structure_term_id,
                        "ei_uuid": expression_unique_key}
                    bio_entity_gene_aos.append(bio_entity_gene_ao)

                    pub = {
                        "ei_uuid": expression_unique_key,
                        "pubPrimaryKey": pub_med_id + publication_mod_id,
                        "pubMedId": pub_med_id,
                        "pubMedUrl": pub_med_url,
                        "pubModId": publication_mod_id,
                        "pubModUrl": pub_mod_url}
                    pubs.append(pub)

                    ao_expression = {
                        "geneId": gene_id,
                        "whenExpressedStage": when_expressed_stage,
                        "pubMedId": pub_med_id,
                        "pubMedUrl": pub_med_url,
                        "pubModId": publication_mod_id,
                        "pubModUrl": pub_mod_url,
                        "pubPrimaryKey": pub_med_id + publication_mod_id,
                        "uuid": str(uuid.uuid4()),
                        "assay": assay,
                        "anatomicalStructureTermId": anatomical_structure_term_id,
                        "whereExpressedStatement": where_expressed_statement,
                        "ei_uuid": expression_unique_key,
                        "ebe_uuid": expression_entity_unique_key}
                    ao_expressions.append(ao_expression)

                    if cellular_component_qualifier_term_id is not None:

                        cc_qualifier = {
                            "ebe_uuid": expression_entity_unique_key,
                            "cellularComponentQualifierTermId": cellular_component_qualifier_term_id
                        }
                        cc_qualifiers.append(cc_qualifier)

                    if anatomical_structure_term_id is None:
                        anatomical_structure_term_id = ""

                        cc_expression = {
                            "geneId": gene_id,
                            "whenExpressedStage": when_expressed_stage,
                            "pubMedId": pub_med_id,
                            "pubMedUrl": pub_med_url,
                            "pubModId": publication_mod_id,
                            "pubModUrl": pub_mod_url,
                            "pubPrimaryKey": pub_med_id + publication_mod_id,
                            "assay": assay,
                            "whereExpressedStatement": where_expressed_statement,
                            "cellularComponentTermId": cellular_component_term_id,
                            "ei_uuid": expression_unique_key,
                            "ebe_uuid": expression_entity_unique_key
                        }
                        cc_expressions.append(cc_expression)

                    if anatomical_structure_qualifier_term_id is not None:
                        ao_qualifier = {
                            "ebe_uuid":
                            expression_entity_unique_key,

                            "anatomicalStructureQualifierTermId":
                            anatomical_structure_qualifier_term_id}

                        ao_qualifiers.append(ao_qualifier)

                    if anatomical_sub_structure_term_id is not None:
                        ao_substructure = {
                            "ebe_uuid":
                            expression_entity_unique_key,

                            "anatomicalSubStructureTermId":
                            anatomical_sub_structure_term_id}

                        ao_substructures.append(ao_substructure)

                    if anatomical_sub_structure_qualifier_term_id is not None:
                        ao_ss_qualifier = {
                            "ebe_uuid":
                            expression_entity_unique_key,

                            "anatomicalSubStructureQualifierTermId":
                            anatomical_sub_structure_qualifier_term_id}

                        ao_ss_qualifiers.append(ao_ss_qualifier)

                    if where_expressed_statement is None:
                        where_expressed_statement = ""

                    if anatomical_structure_term_id is not None \
                            and anatomical_structure_term_id != "" \
                            and cellular_component_term_id is not None \
                            and cellular_component_term_id != "":

                        ao_cc_expression = {
                            "geneId": gene_id,
                            "whenExpressedStage": when_expressed_stage,
                            "pubMedId": pub_med_id,
                            "pubMedUrl": pub_med_url,
                            "pubModId": publication_mod_id,
                            "pubModUrl": pub_mod_url,
                            "pubPrimaryKey": pub_med_id + publication_mod_id,
                            "uuid": str(uuid.uuid4()),
                            "stageTermId": stage_term_id,
                            "stageName": stage_name,
                            "stageUberonTermId": stage_uberon_term_id,
                            "assay": assay,
                            "cellularComponentTermId": cellular_component_term_id,
                            "anatomicalStructureTermId": anatomical_structure_term_id,
                            "whereExpressedStatement": where_expressed_statement,
                            "ei_uuid": expression_unique_key,
                            "ebe_uuid": expression_entity_unique_key}

                        ao_cc_expressions.append(ao_cc_expression)

                if counter == batch_size:
                    yield [bio_entities,
                           bio_entity_gene_aos,
                           bio_join_entities,
                           ao_expressions,
                           cc_expressions,
                           ao_cc_expressions,
                           ao_qualifiers,
                           ao_substructures,
                           ao_ss_qualifiers,
                           cc_qualifiers,
                           stage_list,
                           stage_uberon_data,
                           uberon_ao_data,
                           uberon_ao_other_data,
                           uberon_stage_other_data,
                           cross_references,
                           pubs]
                    bio_entities = []
                    bio_join_entities = []
                    ao_expressions = []
                    cc_expressions = []
                    ao_qualifiers = []
                    ao_substructures = []
                    ao_ss_qualifiers = []
                    cc_qualifiers = []
                    ao_cc_expressions = []
                    stage_list = []
                    uberon_stage_other_data = []
                    stage_uberon_data = []
                    uberon_ao_other_data = []
                    uberon_ao_data = []
                    cross_references = []
                    bio_entity_gene_aos = []
                    pubs = []
                    counter = 0

            if counter > 0:
                yield [bio_entities,
                       bio_entity_gene_aos,
                       bio_join_entities,
                       ao_expressions,
                       cc_expressions,
                       ao_cc_expressions,
                       ao_qualifiers,
                       ao_substructures,
                       ao_ss_qualifiers,
                       cc_qualifiers,
                       stage_list,
                       stage_uberon_data,
                       uberon_ao_data,
                       uberon_ao_other_data,
                       uberon_stage_other_data,
                       cross_references,
                       pubs]
