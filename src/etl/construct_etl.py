'''Construct ETL'''

import logging
import multiprocessing
import uuid
from etl.helpers import ResourceDescriptorHelper
from etl import ETL
from etl.helpers import ETLHelper, TextProcessingHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor


class ConstructETL(ETL):
    '''Construct ETL'''

    logger = logging.getLogger(__name__)
    xrefUrlMap = ResourceDescriptorHelper().get_data()

    construct_query_template = """
          USING PERIODIC COMMIT %s
          LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

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
                   o.dataProvider = row.dataProvider

              MERGE (o)-[:FROM_SPECIES]-(s)

            """

    construct_secondaryids_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (f:Construct {primaryKey:row.data_id})

            MERGE (second:SecondaryId:Identifier {primaryKey:row.secondary_id})
                SET second.name = row.secondary_id
            MERGE (f)-[aka1:ALSO_KNOWN_AS]->(second) """

    construct_synonyms_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (a:Construct {primaryKey:row.data_id})

            MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                SET syn.name = row.synonym
            MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn) """

    construct_xrefs_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Construct {primaryKey:row.dataId}) """ + ETLHelper.get_cypher_xref_text()

    construct_gene_component_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Construct {primaryKey:row.constructID}), (g:Gene {primaryKey:row.componentID})
            CALL apoc.create.relationship(g, row.componentRelation, {}, o) yield rel
            REMOVE rel.noOp"""

    construct_no_gene_component_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Construct {primaryKey:row.constructID}), (g:NonBGIConstructComponent {primaryKey:row.componentSymbol})
            CALL apoc.create.relationship(g, row.componentRelation, {}, o) yield rel
            REMOVE rel.noOp"""

    non_bgi_component_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MERGE (o:NonBGIConstructComponent {primaryKey:row.componentSymbol})"""

    def __init__(self, config):
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
        self.logger.info("Finished Loading Construct Data: %s", sub_type.get_data_provider())

        if data is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [ConstructETL.construct_query_template, commit_size,
             "Construct_data_" + sub_type.get_data_provider() + ".csv"],
            [ConstructETL.construct_secondaryids_template, commit_size,
             "Construct_secondaryids_" + sub_type.get_data_provider() + ".csv"],
            [ConstructETL.construct_synonyms_template, commit_size,
             "Construct_synonyms_" + sub_type.get_data_provider() + ".csv"],
            [ConstructETL.construct_xrefs_template, commit_size, "Construct_xrefs_" + sub_type.get_data_provider() + ".csv"],
            [ConstructETL.non_bgi_component_template, commit_size,
             "Construct_non_bgi_component_" + sub_type.get_data_provider() + ".csv"],
            [ConstructETL.construct_gene_component_template, commit_size, "Construct_components_gene" + sub_type.get_data_provider() + ".csv"],
            [ConstructETL.construct_no_gene_component_template, commit_size,
             "Construct_components_no_gene" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, construct_data, data_provider, batch_size):
        '''Create Generators'''

        dataproviders = []
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

        data_provider_object = construct_data['metaData']['dataProvider']

        data_provider_cross_ref = data_provider_object.get('crossReference')
        data_provider = data_provider_cross_ref.get('id')
        data_provider_pages = data_provider_cross_ref.get('pages')
        data_provider_cross_ref_set = []

        load_key = date_produced + data_provider + "_construct"

        # TODO: get SGD to fix their files.

        if data_provider_pages is not None:
            for data_provider_page in data_provider_pages:
                cross_ref_complete_url = ETLHelper.get_page_complete_url(data_provider,
                                                                         self.xref_url_map,
                                                                         data_provider,
                                                                         data_provider_page)

                dataProviderCrossRefSet.append(ETLHelper.get_xref_dict(data_provider, data_provider, data_provider_page,
                                                                       dataProviderPage, dataProvider,
                                                                       crossRefCompleteUrl,
                                                                       dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                logger.info("data provider: " + dataProvider)

        if 'release' in construct_data['metaData']:
            release = construct_data['metaData']['release']

        for constructRecord in construct_data['data']:
            counter = counter + 1
            globalId = constructRecord['primaryId']
            localId = globalId.split(":")[1]
            modGlobalCrossRefId = ""

            if self.testObject.using_test_data() is True:
                is_it_test_entry = self.testObject.check_for_test_id_entry(globalId)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            nameText = TextProcessingHelper.cleanhtml(constructRecord.get('name'))


            construct_dataset = {
                "symbol": constructRecord.get('name'),
                "primaryId": constructRecord.get('primaryId'),
                "globalId": globalId,
                "localId": localId,
                "dataProviders": dataProviders,
                "dateProduced": dateProduced,
                "loadKey": loadKey,
                "release": release,
                "modGlobalCrossRefId": modGlobalCrossRefId,
                "uuid": str(uuid.uuid4()),
                "dataProvider": data_provider,
                "nameText": nameText
            }
            constructs.append(construct_dataset)

            if 'crossReferences' in constructRecord:

                for crossRef in constructRecord['crossReferences']:
                    crossRefId = crossRef.get('id')
                    local_crossref_id = crossRefId.split(":")[1]
                    prefix = crossRef.get('id').split(":")[0]
                    pages = crossRef.get('pages')

                    # some pages collection have 0 elements
                    if pages is not None and len(pages) > 0:
                        for page in pages:
                            if page == 'construct':
                                modGlobalCrossRefId = ETLHelper.get_page_complete_url(local_crossref_id, self.xrefUrlMap, prefix, page)
                                xref = ETLHelper.get_xref_dict(local_crossref_id, prefix, page, page, crossRefId, modGlobalCrossRefId, crossRefId+page)
                                xref['dataId'] = globalId
                                crossReferenceList.append(xref)

            if 'constructComponents' in constructRecord:
                for component in constructRecord.get('constructComponents'):
                    componentRelation = component.get('componentRelation').upper()
                    componentSymbol = component.get('componentSymbol')
                    componentID = component.get('componentID')

                    if componentID is not None:
                        componentDetail = {
                            "componentRelation": componentRelation.upper(),
                            "componentSymbol": componentSymbol,
                            "componentID": componentID,
                            "constructID": constructRecord.get('primaryId')
                        }
                        componentDetails.append(componentDetail)
                    else:
                        componentDetail = {
                            "componentRelation": componentRelation.upper(),
                            "componentSymbol": componentSymbol,
                            "constructID": constructRecord.get('primaryId')
                        }
                        nonBgiComponent = {"componentSymbol": componentSymbol}
                        nonBgiComponents.append(nonBgiComponent)
                        componentNoGeneDetails.append(componentDetail)

            if 'synonyms' in constructRecord:
                for syn in constructRecord.get('synonyms'):
                    construct_synonym = {
                        "data_id": constructRecord.get('primaryId'),
                        "synonym": syn.strip()
                    }
                    construct_synonyms.append(construct_synonym)

            if 'secondaryIds' in constructRecord:
                for secondaryId in constructRecord.get('secondaryIds'):
                    construct_secondaryId = {
                        "data_id": constructRecord.get('primaryId'),
                        "secondary_id": secondaryId
                    }
                    construct_secondaryIds.append(construct_secondaryId)

            if counter == batch_size:
                yield [constructs, construct_secondaryIds, construct_synonyms, crossReferenceList, nonBgiComponents, componentDetails, componentNoGeneDetails]
                constructs = []
                construct_secondaryIds = []
                construct_synonyms = []
                crossReferenceList = []
                nonBgiComponents = []
                componentDetails = []
                componentNoGeneDetails = []
                counter = 0

        if counter > 0:
            yield [constructs, construct_secondaryIds, construct_synonyms, crossReferenceList, nonBgiComponents, componentDetails, componentNoGeneDetails]
