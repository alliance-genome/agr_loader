"""Disease ETL."""

# TODO need to fix the difference between disaeseRecord and disease_record in original code

import logging
import multiprocessing
import uuid
from etl import ETL
from etl.helpers import ETLHelper
from etl.helpers import Neo4jHelper
from files import JSONFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class DiseaseETL(ETL):
    """Disease ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    execute_annotation_xrefs_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:DiseaseEntityJoin:Association {primaryKey:row.dataId})
        """ + ETLHelper.get_cypher_xref_text_annotation_level()

    execute_exp_condition_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MERGE (ec:ExperimentalCondition {primaryKey:row.ecUniqueKey})
                ON CREATE SET ec.conditionClassId     = row.conditionClassId,
                              ec.anatomicalOntologyId = row.anatomicalOntologyId,
                              ec.chemicalOntologyId   = row.chemicalOntologyId,
                              ec.geneOntologyId       = row.geneOntologyId,
                              ec.NCBITaxonID          = row.NCBITaxonID,
                              ec.conditionStatement   = row.conditionStatement
    """

    execute_exp_condition_relations_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (dfa:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
        MATCH (ec:ExperimentalCondition {primaryKey:row.ecUniqueKey})

        MERGE (dfa)-[rel:ASSOCIATION]-(ec)
            ON CREATE SET rel.conditionRelationType = row.conditionRelationType,
                          rel.isModifier = row.isModifier,
                          rel.conditionQuantity = row.conditionQuantity
    """

    execute_agms_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            // GET PRIMARY DATA OBJECTS

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (agm:AffectedGenomicModel {primaryKey:row.primaryId})

            //Intentional MERGEing (preventing duplicates), please leave as is

            CALL apoc.merge.relationship(d, row.relationshipType, {uuid: row.diseaseUniqueKey}, {}, agm) yield rel
            REMOVE rel.noOp

            MERGE (dfa:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
                ON CREATE SET dfa.dataProvider = row.dataProvider,
                              dfa.sortOrder = 1,
                              dfa.joinType = row.relationshipType,
                              dfa.negation = row.negation

            MERGE (agm)-[fdaf:ASSOCIATION]->(dfa)
            MERGE (dfa)-[dadf:ASSOCIATION]->(d)

            // PUBLICATIONS FOR FEATURE

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl

            MERGE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
                ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join',
                                pubEJ.dateAssigned = row.dateAssigned

            MERGE (dfa)-[dapug:EVIDENCE {uuid:row.pecjPrimaryKey}]->(pubEJ)

            MERGE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)
            """

    execute_allele_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            // GET PRIMARY DATA OBJECTS

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (allele:Allele:Feature {primaryKey:row.primaryId})

            CALL apoc.create.relationship(d, row.relationshipType, {}, allele) yield rel
                        SET rel.uuid = row.diseaseUniqueKey
            REMOVE rel.noOp

            //This is an intentional MERGE, please leave as is

            MERGE (dfa:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
                ON CREATE SET dfa.dataProvider = row.dataProvider,
                              dfa.sortOrder = 1,
                              dfa.joinType = row.relationshipType,
                              dfa.negation = row.negation

            MERGE (allele)-[fdaf:ASSOCIATION]->(dfa)
            MERGE (dfa)-[dadf:ASSOCIATION]->(d)

            // PUBLICATIONS FOR FEATURE

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId,
                 pubf.pubMedId = row.pubMedId,
                 pubf.pubModUrl = row.pubModUrl,
                 pubf.pubMedUrl = row.pubMedUrl

            MERGE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
                ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join',
                                pubEJ.dateAssigned = row.dateAssigned

            MERGE (dfa)-[dapug:EVIDENCE {uuid:row.pecjPrimaryKey}]->(pubEJ)

            MERGE (pubf)-[pubfpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)"""

    execute_gene_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (d:DOTerm:Ontology {primaryKey:row.doId})
            MATCH (gene:Gene {primaryKey:row.primaryId})

            CALL apoc.create.relationship(d, row.relationshipType, {}, gene) yield rel
                        SET rel.uuid = row.diseaseUniqueKey
            REMOVE rel.noOp

            MERGE (dga:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})
                SET dga.dataProvider = row.dataProvider,
                    dga.sortOrder = 1,
                    dga.joinType = row.relationshipType,
                    dga.negation = row.negation


            MERGE (gene)-[fdag:ASSOCIATION]->(dga)
            MERGE (dga)-[dadg:ASSOCIATION]->(d)

            // PUBLICATIONS FOR GENE

            MERGE (pubg:Publication {primaryKey:row.pubPrimaryKey})
                SET pubg.pubModId = row.pubModId,
                    pubg.pubMedId = row.pubMedId,
                    pubg.pubModUrl = row.pubModUrl,
                    pubg.pubMedUrl = row.pubMedUrl

            MERGE (pubEJ:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
            ON CREATE SET pubEJ.joinType = 'pub_evidence_code_join',
                                pubEJ.dateAssigned = row.dateAssigned

            MERGE (dga)-[dapug:EVIDENCE {uuid:row.pecjPrimaryKey}]->(pubEJ)
            MERGE (pubg)-[pubgpubEJ:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(pubEJ)"""

    execute_ecode_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Ontology:ECOTerm {primaryKey:row.ecode})
            MATCH (pubjk:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})
            MERGE (pubjk)-[daecode1g:ASSOCIATION {uuid:row.pecjPrimaryKey}]->(o)"""

    execute_withs_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (dga:Association:DiseaseEntityJoin {primaryKey:row.diseaseUniqueKey})

            MATCH (diseaseWith:Gene {primaryKey:row.withD})
            MERGE (dga)-[dgaw:FROM_ORTHOLOGOUS_GENE]-(diseaseWith) """

    execute_pges_gene_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (n:Gene {primaryKey:row.pgeId})
            MATCH (d:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})

            MERGE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]-(n)"""

    execute_pges_allele_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (n:Allele {primaryKey:row.pgeId})
            MATCH (d:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})

            MERGE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]-(n)"""

    execute_pges_agm_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (n:AffectedGenomicModel {primaryKey:row.pgeId})
            MATCH (d:PublicationJoin:Association {primaryKey:row.pecjPrimaryKey})

            MERGE (d)-[dgaw:PRIMARY_GENETIC_ENTITY]-(n)"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config
        self.disease_unique_key = None
        self.disease_association_type = None

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

        self.delete_empty_nodes()

    def delete_empty_nodes(self):
        """Delete Empty Nodes."""
        self.logger.debug("delete empty nodes")

        delete_empty_do_nodes_query = """
                MATCH (dd:DOTerm)
                WHERE keys(dd)[0] = 'primaryKey'
                      AND size(keys(dd)) = 1
                DETACH DELETE (dd)"""

        Neo4jHelper.run_single_query(delete_empty_do_nodes_query)

    def _process_sub_type(self, sub_type):

        self.logger.info("Loading Disease Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        self.logger.info("Finished Loading Disease Data: %s", sub_type.get_data_provider())

        if data is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
            return

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [self.execute_allele_query_template, commit_size,
             "disease_allele_data_" + sub_type.get_data_provider() + ".csv"],
            [self.execute_gene_query_template, commit_size,
             "disease_gene_data_" + sub_type.get_data_provider() + ".csv"],
            [self.execute_exp_condition_query_template, commit_size,
             "disease_exp_condition_data_" + sub_type.get_data_provider() + ".csv"],
            [self.execute_agms_query_template, commit_size,
             "disease_agms_data_" + sub_type.get_data_provider() + ".csv"],
            [self.execute_exp_condition_relations_query_template, commit_size,
             "disease_exp_condition_rel_data_" + sub_type.get_data_provider() + ".csv"],
            [self.execute_pges_gene_query_template, commit_size,
             "disease_pges_gene_data_" + sub_type.get_data_provider() + ".csv"],
            [self.execute_pges_allele_query_template, commit_size,
             "disease_pges_allele_data_" + sub_type.get_data_provider() + ".csv"],
            [self.execute_pges_agm_query_template, commit_size,
             "disease_pges_agms_data_" + sub_type.get_data_provider() + ".csv"],
            [self.execute_withs_query_template, commit_size,
             "disease_withs_data_" + sub_type.get_data_provider() + ".csv"],
            [self.execute_ecode_query_template, commit_size,
             "disease_evidence_code_data_" + sub_type.get_data_provider() + ".csv"],
            [self.execute_annotation_xrefs_query_template, commit_size,
             "disease_annotation_xrefs_data_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data, batch_size, sub_type.get_data_provider())

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("Disease-{}: ".format(sub_type.get_data_provider()))
        self.logger.info("Finished Loading Disease Data: %s", sub_type.get_data_provider())

    def process_pages(self, dp, xrefs, pages):
        """Process pages to get xrefs."""
        annotation_type = dp.get('type')
        xref = dp.get('crossReference')
        cross_ref_id = xref.get('id')
        if ":" in cross_ref_id:
            local_crossref_id = cross_ref_id.split(":")[1]
            prefix = cross_ref_id.split(":")[0]
        else:
            local_crossref_id = ""
            prefix = cross_ref_id

        if annotation_type is None:
            annotation_type = 'curated'

        for page in pages:
            if (self.data_provider == 'RGD' or self.data_provider == 'HUMAN') and prefix == 'DOID':
                display_name = 'RGD'
            elif (self.data_provider == 'RGD' or self.data_provider == 'HUMAN') and prefix == 'OMIM':
                display_name = 'OMIM'
            else:
                display_name = cross_ref_id.split(":")[0]
                if display_name == 'DOID':
                    display_name = self.data_provider

            mod_global_cross_ref_url = self.etlh.rdh2.return_url_from_key_value(
                prefix, local_crossref_id, page)
            passing_xref = ETLHelper.get_xref_dict(
                local_crossref_id, prefix, page, page,
                display_name, mod_global_cross_ref_url,
                cross_ref_id + page + annotation_type)
            passing_xref['dataId'] = self.disease_unique_key

            if 'loaded' in annotation_type:
                passing_xref['loadedDB'] = 'true'
                passing_xref['curatedDB'] = 'false'
            else:
                passing_xref['curatedDB'] = 'true'
                passing_xref['loadedDB'] = 'false'

            xrefs.append(passing_xref)

    def xrefs_process(self, disease_record, xrefs):
        """Process the xrefs."""
        if 'dataProvider' not in disease_record:
            return

        for dp in disease_record['dataProvider']:
            xref = dp.get('crossReference')
            pages = xref.get('pages')

            if pages is None or len(pages) == 0:
                continue
            self.process_pages(dp, xrefs, pages)

    def evidence_process(self, disease_record, pubs, evidence_code_list_to_yield):
        """Process evidence."""
        pecj_primary_key = str(uuid.uuid4())
        if 'evidence' not in disease_record:
            self.logger.critical("No evidence but creating new pecj_primary_key anyway")
            return pecj_primary_key
        evidence = disease_record.get('evidence')
        if 'publication' in evidence:
            pecj_primary_key = str(uuid.uuid4())
            publication = evidence.get('publication')
            if publication.get('publicationId').startswith('PMID:'):
                pubs['pub_med_id'] = publication.get('publicationId')
                pubs['pub_med_url'] = self.etlh.return_url_from_identifier(pubs['pub_med_id'])
                if 'crossReference' in publication:
                    pub_xref = publication.get('crossReference')
                    pubs['publication_mod_id'] = pub_xref.get('id')
                    pubs['pub_mod_url'] = self.etlh.return_url_from_identifier(pubs['publication_mod_id'])
            else:
                pubs['publication_mod_id'] = publication.get('publicationId')
                pubs['pub_mod_url'] = self.etlh.return_url_from_identifier(pubs['publication_mod_id'])

        if 'evidenceCodes' in disease_record['evidence']:
            for ecode in disease_record['evidence'].get('evidenceCodes'):
                ecode_map = {"pecjPrimaryKey": pecj_primary_key,
                             "ecode": ecode}
                evidence_code_list_to_yield.append(ecode_map)
        return pecj_primary_key

    def objectrelation_process(self, disease_record):
        """Object Relation processing."""
        negation = ''
        if 'objectRelation' not in disease_record:
            self.logger.critical("objectRelation not in record so disease_annotation_type is the last one seen")
            return negation, None

        if 'negation' in disease_record:
            # this capitalization is purposeful
            if self.disease_association_type == 'IS_IMPLICATED_IN':
                self.disease_association_type = 'IS_NOT_IMPLICATED_IN'
            elif self.disease_association_type == 'IS_MODEL_OF':
                self.disease_association_type = 'IS_NOT_MODEL_OF'
            elif self.disease_association_type == 'IS_MARKER_FOR':
                self.disease_association_type = 'IS_NOT_MARKER_FOR'
            negation = 'NOT'
            self.disease_unique_key = self.disease_unique_key + negation

        return negation
    # Not used anywhere so commented out for now?
    #     additional_genetic_components = []

    #     if 'additionalGeneticComponents' in disease_record['objectRelation']:
    #         for component in disease_record['objectRelation']['additionalGeneticComponents']:
    #             component_symbol = component.get('componentSymbol')
    #             component_id = component.get('componentId')
    #             component_url = component.get('componentUrl') + component_id
    #             additional_genetic_components.append(
    #                 {"id": component_id,
    #                  "componentUrl": component_url,
    #                  "componentSymbol": component_symbol}
    #             )

    def conditionrelations_process(self, exp_conditions, disease_record):
        """condition relations processing."""

        condition_relations = []

        if 'conditionRelations' not in disease_record:
            # No condition relation annotation to parse
            return condition_relations

        for relation in disease_record['conditionRelations']:
            for condition in relation['conditions']:
                # Store unique conditions
                # Unique condition key: conditionClassId + (anatomicalOntologyId | chemicalOntologyId | geneOntologyId | NCBITaxonID)
                unique_key = condition.get('conditionClassId') \
                              + str( condition.get('conditionId') or '' ) \
                              + str( condition.get('anatomicalOntologyId') or '' ) \
                              + str( condition.get('chemicalOntologyId') or '' ) \
                              + str( condition.get('geneOntologyId') or '' ) \
                              + str( condition.get('NCBITaxonID') or '' )

                if unique_key not in exp_conditions:
                    condition_dataset = {
                        "ecUniqueKey": unique_key,
                        "conditionClassId":     condition.get('conditionClassId'),
                        'anatomicalOntologyId': condition.get('anatomicalOntologyId'),
                        'chemicalOntologyId':   condition.get('chemicalOntologyId'),
                        'geneOntologyId':       condition.get('geneOntologyId'),
                        'NCBITaxonID':          condition.get('NCBITaxonID'),
                        'conditionStatement':   condition.get('conditionStatement')
                    }

                    exp_conditions[unique_key] = condition_dataset

                # Store the relation between condition and disease_record
                is_modifier = False
                if relation.get('conditionRelationType') != "has_condition":
                    is_modifier = True

                relation_dataset = {
                    'ecUniqueKey': unique_key,
                    'conditionRelationType': relation.get('conditionRelationType'),
                    'isModifier': is_modifier,
                    'conditionQuantity': condition.get('conditionQuantity'),
                    # diseaseUniqueKey to be appended after fn completion, as the combination
                    #  of all conditions defines a unique object (and thus diseaseUniqueKey)
                }

                condition_relations.append(relation_dataset)
        return condition_relations

    def withs_process(self, disease_record, withs):
        """Process withs."""
        if 'with' not in disease_record:
            return
        with_record = disease_record.get('with')
        for rec in with_record:
            self.disease_unique_key = self.disease_unique_key + rec
        for rec in with_record:
            with_map = {
                "diseaseUniqueKey": self.disease_unique_key,
                "withD": rec
                }
            withs.append(with_map)

    def primary_genetic_entity_process(self, disease_record, pge_list_to_yield, pecj_primary_key):
        """Primary Genetic Entity ID process."""
        if 'primaryGeneticEntityIDs' not in disease_record:
            return

        pge_ids = disease_record.get('primaryGeneticEntityIDs')
        for pge in pge_ids:
            # ? pge_key = pge_key + pge
            pge_map = {"pecjPrimaryKey": pecj_primary_key,
                       "pgeId": pge}
            pge_list_to_yield.append(pge_map)

    def get_generators(self, disease_data, batch_size, data_provider):
        """Create generators."""
        counter = 0
        gene_list_to_yield = []
        allele_list_to_yield = []
        exp_condition_dict = dict()
        agm_list_to_yield = []
        cond_rels_to_yield = []
        evidence_code_list_to_yield = []
        withs = []
        pge_list_to_yield = []
        xrefs = []

        self.data_providers_process(disease_data)

        for disease_record in disease_data['data']:

            pubs = {'pub_med_url': None,
                    'pub_med_id': "",
                    'pub_mod_url': None,
                    'publication_mod_id': ""
                    }
            # pge_key = ''

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(disease_record.get('objectId'))
                if is_it_test_entry is False:
                    continue

            self.disease_association_type = disease_record['objectRelation'].get("associationType").upper()

            record_cond_relations = self.conditionrelations_process(exp_condition_dict, disease_record)

            # disease_unique_key formatted to represent (readably):
            #  object `a` under conditions `b` has relation `c` to disease `d`
            # Including a combination of unique condition keys in the disease_unique_key,
            #  in order to create unique DiseaseEntityJoin nodes per condition combo
            #  (to which the appropriate evidence papers can be linked).

            #object `a`
            self.disease_unique_key = disease_record.get('objectId')

            #conditions `b` (sorted, to ensure consistent diseaseUniqueKey!)
            for cond_rel in sorted(record_cond_relations, key=lambda rel: rel["ecUniqueKey"]):
                self.disease_unique_key += cond_rel["ecUniqueKey"]

            #relation `c` to disease `d`
            self.disease_unique_key += self.disease_association_type + disease_record.get('DOid')

            #Add this disease_unique_key to every experimental condition relation
            for cond_rel in record_cond_relations:
                cond_rel["diseaseUniqueKey"] = self.disease_unique_key

            # and extend the cond_rels_to_yield with these (now completed) relations
            cond_rels_to_yield.extend(record_cond_relations)

            counter = counter + 1
            disease_object_type = disease_record['objectRelation'].get("objectType")

            primary_id = disease_record.get('objectId')
            do_id = disease_record.get('DOid')

            self.xrefs_process(disease_record, xrefs)
            pecj_primary_key = self.evidence_process(disease_record, pubs, evidence_code_list_to_yield)

            negation = self.objectrelation_process(disease_record)

            self.withs_process(disease_record, withs)
            self.primary_genetic_entity_process(disease_record, pge_list_to_yield, pecj_primary_key)

            self.xrefs_process(disease_record, xrefs)

            disease_record = {
                "diseaseUniqueKey": self.disease_unique_key,
                "doId": do_id,
                "primaryId": primary_id,
                "pecjPrimaryKey": pecj_primary_key,
                "relationshipType": self.disease_association_type,
                "dataProvider": data_provider,
                "dateAssigned": disease_record.get("dateAssigned"),
                "pubPrimaryKey": pubs['publication_mod_id'] + pubs['pub_med_id'],
                "pubModId": pubs['publication_mod_id'],
                "pubMedId": pubs['pub_med_id'],
                "pubMedUrl": pubs['pub_med_url'],
                "pubModUrl": pubs['pub_mod_url'],
                "negation": negation}

            if disease_object_type == 'gene':
                gene_list_to_yield.append(disease_record)
            elif disease_object_type == 'allele':
                allele_list_to_yield.append(disease_record)
            else:
                agm_list_to_yield.append(disease_record)

            if counter == batch_size:
                yield [allele_list_to_yield,
                       gene_list_to_yield,
                       exp_condition_dict.values(),
                       agm_list_to_yield,
                       cond_rels_to_yield,
                       pge_list_to_yield,
                       pge_list_to_yield,
                       pge_list_to_yield,
                       withs,
                       evidence_code_list_to_yield,
                       xrefs]
                agm_list_to_yield = []
                allele_list_to_yield = []
                gene_list_to_yield = []
                exp_condition_dict = dict()
                cond_rels_to_yield = []
                evidence_code_list_to_yield = []
                pge_list_to_yield = []
                xrefs = []
                withs = []
                counter = 0

        if counter > 0:
            yield [allele_list_to_yield,
                   gene_list_to_yield,
                   exp_condition_dict.values(),
                   agm_list_to_yield,
                   cond_rels_to_yield,
                   pge_list_to_yield,
                   pge_list_to_yield,
                   pge_list_to_yield,
                   withs,
                   evidence_code_list_to_yield,
                   xrefs]
