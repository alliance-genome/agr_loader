"""Construct ETL."""

import logging
import multiprocessing
import uuid
from etl import ETL
from etl.helpers import ETLHelper
from etl.helpers import TextProcessingHelper
from files import JSONFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class ConstructETL(ETL):
    """Construct ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    construct_query_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row

                //Create the Construct node and set properties. primaryKey is required.
                MERGE (o:Construct {primaryKey:row.primaryId})
                    ON CREATE SET o.name = row.name,
                    o.dateProduced = row.dateProduced,
                    o.release = row.release,
                    o.localId = row.localId,
                    o.globalId = row.globalId,
                    o.uuid = row.uuid,
                    o.nameText = row.nameText,
                    o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                    o.dataProviders = row.dataProviders,
                    o.dataProvider = row.dataProvider,
                    o.symbol = row.symbol
            }
        IN TRANSACTIONS of %s ROWS"""

    construct_secondary_ids_query_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row

                MATCH (f:Construct {primaryKey:row.data_id})

                MERGE (second:SecondaryId {primaryKey:row.secondary_id})
                    SET second.name = row.secondary_id
                MERGE (f)-[aka1:ALSO_KNOWN_AS]->(second)
            }
        IN TRANSACTIONS of %s ROWS"""
    
    construct_synonyms_query_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row

                MATCH (a:Construct {primaryKey:row.data_id})

                MERGE(syn:Synonym {primaryKey:row.synonym})
                    SET syn.name = row.synonym
                MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn)
            }
        IN TRANSACTIONS of %s ROWS"""
    
    construct_xrefs_query_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row

                MATCH (o:Construct {primaryKey:row.dataId}) 
                """ + ETLHelper.get_cypher_xref_text() + """
            }
        IN TRANSACTIONS of %s ROWS"""

    construct_gene_component_query_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row

                MATCH (o:Construct {primaryKey:row.constructID}), (g:Gene {primaryKey:row.componentID})
                CALL apoc.create.relationship(g, row.componentRelation, {}, o) yield rel
                REMOVE rel.noOp
            }
        IN TRANSACTIONS of %s ROWS"""

    construct_no_gene_component_query_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row

                MATCH (o:Construct {primaryKey:row.constructID}), (g:NonBGIConstructComponent {primaryKey:row.componentSymbol})
                CALL apoc.create.relationship(g, row.componentRelation, {}, o) yield rel
                REMOVE rel.noOp            
            }
        IN TRANSACTIONS of %s ROWS"""
    
    non_bgi_component_query_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row
                
                MERGE (o:NonBGIConstructComponent {primaryKey:row.componentSymbol})
            }
        IN TRANSACTIONS of %s ROWS"""
    
    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):

        self.logger.info("Loading Construct Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)

        if data is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
            return
        ETLHelper.load_release_info(data, sub_type, self.logger)

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [ConstructETL.construct_query_template,
             "Construct_data_" + sub_type.get_data_provider() + ".csv", commit_size],
            [ConstructETL.construct_secondary_ids_query_template, 
             "Construct_secondary_ids_" + sub_type.get_data_provider() + ".csv", commit_size],
            [ConstructETL.construct_synonyms_query_template, 
             "Construct_synonyms_" + sub_type.get_data_provider() + ".csv", commit_size],
            [ConstructETL.construct_xrefs_query_template, 
             "Construct_xrefs_" + sub_type.get_data_provider() + ".csv", commit_size],
            [ConstructETL.non_bgi_component_query_template, 
             "Construct_non_bgi_component_" + sub_type.get_data_provider() + ".csv", commit_size],
            [ConstructETL.construct_gene_component_query_template, 
             "Construct_components_gene" + sub_type.get_data_provider() + ".csv", commit_size],
            [ConstructETL.construct_no_gene_component_query_template, 
             "Construct_components_no_gene" + sub_type.get_data_provider() + ".csv", commit_size]
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("Construct-{}: ".format(sub_type.get_data_provider()))
        self.logger.info("Finished Loading Construct Data: %s", sub_type.get_data_provider())

    def secondary_process(self, secondarys, data_record):
        """Get secondary ids.

        secondarys: list of dataset items.
        data_record: record to process.
        """
        if data_record.get('secondaryIds') is None:
            return
        for sid in data_record.get('secondaryIds'):
            secondary_id_dataset = {
                "data_id": data_record.get('primaryId'),
                "secondary_id": sid
            }
            secondarys.append(secondary_id_dataset)

    def synonyms_process(self, synonyms, data_record):
        """Get synonyms."""
        if data_record.get('synonyms') is None:
            return
        for syn in data_record.get('synonyms'):
            syn_dataset = {
                "data_id": data_record.get('primaryId'),
                "synonym": syn.strip()
            }
            synonyms.append(syn_dataset)

    def xref_process(self, construct_record, cross_reference_list):
        """Process the xrefs."""
        global_id = construct_record['primaryId']
        if 'crossReferences' not in construct_record:
            return
        for cross_ref in construct_record.get('crossReferences'):
            cross_ref_id = cross_ref.get('id')
            local_crossref_id = cross_ref_id.split(":")[1]
            prefix = cross_ref.get('id').split(":")[0]
            pages = cross_ref.get('pages')

            # some pages collection have 0 elements
            if pages is None or len(pages) == 0:
                continue
            for page in pages:
                if page == 'construct':
                    mod_global_cross_ref_id = self.etlh.rdh2.return_url_from_key_value(
                        prefix, local_crossref_id, page)
                    xref = ETLHelper.get_xref_dict(
                        local_crossref_id,
                        prefix,
                        page,
                        page,
                        cross_ref_id,
                        mod_global_cross_ref_id,
                        cross_ref_id + page)
                    xref['dataId'] = global_id
                    cross_reference_list.append(xref)

    def comp_process(self, construct_record, component_details, non_bgi_components, component_no_gene_details):
        """Process components."""
        if 'constructComponents' not in construct_record:
            return
        for component in construct_record.get('constructComponents'):
            component_relation = component.get('componentRelation').upper()
            component_symbol = component.get('componentSymbol')
            component_id = component.get('componentID')

            component_detail = {
                "componentRelation": component_relation.upper(),
                "componentSymbol": component_symbol,
                "constructID": construct_record.get('primaryId')
            }
            if component_id is not None:
                component_detail["componentID"] = component_id
                component_details.append(component_detail)
            else:
                non_bgi_component = {"componentSymbol": component_symbol}
                non_bgi_components.append(non_bgi_component)
                component_no_gene_details.append(component_detail)

    def get_generators(self, construct_data, data_provider, batch_size):
        """Create Generators."""
        release = ""
        constructs = []
        construct_synonyms = []
        construct_secondary_ids = []
        cross_reference_list = []
        component_details = []
        component_no_gene_details = []
        non_bgi_components = []

        counter = 0
        date_produced = construct_data['metaData']['dateProduced']

        load_key = date_produced + data_provider + "_construct"

        # TODO: get SGD to fix their files.
        self.data_providers_process(construct_data)

        if 'release' in construct_data['metaData']:
            release = construct_data['metaData']['release']

        for construct_record in construct_data['data']:

            counter = counter + 1
            global_id = construct_record['primaryId']
            local_id = global_id.split(":")[1]
            mod_global_cross_ref_id = ""

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(global_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            name_text = TextProcessingHelper.cleanhtml(construct_record.get('name'))

            construct_dataset = {
                "symbol": construct_record.get('name'),
                "primaryId": construct_record.get('primaryId'),
                "globalId": global_id,
                "localId": local_id,
                "dataProviders": self.data_providers,
                "dateProduced": date_produced,
                "loadKey": load_key,
                "release": release,
                "modGlobalCrossRefId": mod_global_cross_ref_id,
                "uuid": str(uuid.uuid4()),
                "dataProvider": data_provider,
                "nameText": name_text,
                "name": construct_record.get('name')
            }
            constructs.append(construct_dataset)

            self.xref_process(construct_record, cross_reference_list)
            self.synonyms_process(construct_synonyms, construct_record)
            self.secondary_process(construct_secondary_ids, construct_record)
            self.comp_process(construct_record, component_details, non_bgi_components, component_no_gene_details)

            if counter == batch_size:
                yield [constructs,
                       construct_secondary_ids,
                       construct_synonyms,
                       cross_reference_list,
                       non_bgi_components,
                       component_details,
                       component_no_gene_details]
                constructs = []
                construct_secondary_ids = []
                construct_synonyms = []
                cross_reference_list = []
                non_bgi_components = []
                component_details = []
                component_no_gene_details = []
                counter = 0

        if counter > 0:
            yield [constructs,
                   construct_secondary_ids,
                   construct_synonyms,
                   cross_reference_list,
                   non_bgi_components,
                   component_details,
                   component_no_gene_details]
