import logging
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)


class HTPMetaDatasetETL(ETL):
    htp_dataset_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        CREATE (ds:HTPDataset {primaryKey:row.datasetId})
          SET ds.dateAssigned = row.dateAssigned,
              ds.summary = row.summary,
              ds.numChannels = row.numChannels,
              ds.subSeries = row.subSeries,
              ds.title = row.title,
              ds.crossRefCompleteUrl = row.crossRefCompleteUrl,
              ds.dataProvider = row.dataProvider
         """

    htp_dataset_pub_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (ds:HTPDataset {primaryKey: row.datasetId})

        MERGE (p:Publication {primaryKey:row.pubPrimaryKey})
            ON CREATE SET p.pubModId = row.pubModId,
                          p.pubMedId = row.pubMedId,
                          p.pubModUrl = row.pubModUrl,
                          p.pubMedUrl = row.pubMedUrl

        MERGE (p)-[:ASSOCIATION]-(ds)

    """

    htp_category_tags_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (ds:HTPDataset {primaryKey:row.datasetId})

        MERGE (ct:CategoryTag {primaryKey:row.tag})

        MERGE (ds)-[:CATEGORY_TAG]-(ct)    

    """

    htp_secondaryIds_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (ds:HTPDataset {primaryKey:row.datasetId})

        MERGE (s:SecondaryId:Identifier {primaryKey:row.secondaryId})
                ON CREATE SET s.name = row.secondaryId

        MERGE (ds)-[aka:ALSO_KNOWN_AS]-(s)


    """

    htpdataset_xrefs_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (o:HTPDataset {primaryKey:row.dataId}) """ + ETLHelper.get_cypher_preferred_xref_text()

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):

        logger.info("Loading HTP metadata Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        logger.info(filepath)
        data = JSONFile().get_data(filepath)
        logger.info("Finished Loading HTP metadata Data: %s" % sub_type.get_data_provider())

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [HTPMetaDatasetETL.htp_dataset_query_template, commit_size,
             "htp_metadataset_" + sub_type.get_data_provider() + ".csv"],
            [HTPMetaDatasetETL.htp_category_tags_query_template, commit_size,
             "htp_metadataset_tags_" + sub_type.get_data_provider() + ".csv"],
            [HTPMetaDatasetETL.htp_dataset_pub_query_template, commit_size,
             "htp_metadataset_publications_" + sub_type.get_data_provider() + ".csv"],
            [HTPMetaDatasetETL.htpdataset_xrefs_template, commit_size,
             "htp_metadataset_xrefs_" + sub_type.get_data_provider() + ".csv"],
            [HTPMetaDatasetETL.htp_secondaryIds_query_template, commit_size,
             "htp_metadataset_secondaryIds_" + sub_type.get_data_provider() + ".csv"],

        ]

        # Obtain the generator
        generators = self.get_generators(data, batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_cross_references (self, cross_refs, cross_reference_list, datasetId, preferred):
        if cross_refs is not None:
            for cross_ref in cross_refs:
                cross_ref_id = cross_ref.get('id')
                local_cross_ref_id = cross_ref_id.split(":")[1]
                prefix = cross_ref.get('id').split(":")[0]
                pages = cross_ref.get('pages')
                global_xref_id = cross_ref.get('id')

                # some pages collection have 0 elements
                if pages is not None and len(pages) > 0:
                    for page in pages:
                        display_name = ""

                        cross_ref_complete_url = self.etlh.rdh2.return_url_from_key_value(
                            prefix, local_cross_ref_id, page)

                        xref_map = ETLHelper.get_xref_dict(
                            local_cross_ref_id,
                            prefix,
                            page,
                            page,
                            display_name,
                            cross_ref_complete_url,
                            global_xref_id + page)
                        xref_map['dataId'] = datasetId
                        xref_map['preferred'] = preferred
                        cross_reference_list.append(xref_map)

    def get_generators(self, htp_dataset_data, batch_size):
        dataset_tags = []
        htp_datasets = []
        publications = []
        secondaryIds = []
        cross_reference_list = []
        counter = 0

        data_provider_object = htp_dataset_data['metaData']['dataProvider']

        data_provider_cross_ref = data_provider_object.get('crossReference')
        data_provider = data_provider_cross_ref.get('id')

        for dataset_record in htp_dataset_data['data']:

            counter = counter + 1

            dataset = dataset_record.get('datasetId')
            datasetId = dataset.get('primaryId')
            if 'secondaryIds' in dataset:
                for secId in dataset.get('secondaryIds'):
                    secid = {
                        "datasetId": datasetId,
                        "secondaryId": secId
                    }
                    secondaryIds.append(secid)

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(datasetId)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            cross_refs = dataset.get('crossReferences')

            if cross_refs is not None:
                self.get_cross_references(cross_refs, cross_reference_list, datasetId)

            preferred_cross_reference = dataset.get('preferredCrossReference')

            self.get_cross_references(preferred_cross_reference, cross_reference_list, datasetId, 'true')

            self.get_cross_references(cross_refs, cross_reference_list, datasetId, 'false')

            globalPrimaryIdCrossRefId = datasetId
            prefix = globalPrimaryIdCrossRefId.split(":")[0]
            page = 'htp/dataset'
            local_cross_ref_id = globalPrimaryIdCrossRefId.split(":")[1]

            cross_ref_complete_url = self.etlh.rdh2.return_url_from_key_value(
                prefix, local_cross_ref_id, page)


            category_tags = dataset_record.get('categoryTags')

            if category_tags is not None:
                for tag in category_tags :
                    dataset_category_tag = {
                        "datasetId": datasetId,
                        "tag": tag
                    }
                    dataset_tags.append(dataset_category_tag)

            publicationNew = dataset_record.get('publications')
            if publicationNew is not None:
                for pub in publicationNew:
                    pid = pub.get('publicationId')
                    publication_mod_id = ""
                    pub_med_id = ""
                    pub_mod_url = ""
                    pub_med_url = ""
                    if pid is not None and pid.startswith('PMID:'):
                        pub_med_id = pid
                        local_pub_med_id = pub_med_id.split(":")[1]
                        pub_med_url = pub_med_url = self.etlh.get_no_page_complete_url(local_pub_med_id, 'PMID', pub_med_id)
                        if 'crossReference' in pub:
                            pub_xref = pub.get('crossReference')
                            publication_mod_id = pub_xref.get('id')
                            pub_mod_url = self.etlh.rdh2.return_url_from_identifier(publication_mod_id)
                    elif pid is not None and not pid.startswith('PMID:'):
                        publication_mod_id = pub.get('publicationId')
                        pub_mod_url = self.etlh.rdh2.return_url_from_identifier(publication_mod_id)

                    publication = {
                        "datasetId": datasetId,
                        "pubPrimaryKey": publication_mod_id + pub_med_id,
                        "pubModId": publication_mod_id,
                        "pubMedId": pub_med_id,
                        "pubMedUrl": pub_med_url,
                        "pubModUrl": pub_mod_url
                    }
                    publications.append(publication)

            htp_dataset = {
                "datasetId": datasetId,
                "dateAssigned": dataset_record.get('dateAssigned'),
                "title": dataset_record.get('title'),
                "summary": dataset_record.get('summary'),
                "numChannels": dataset_record.get('numChannels'),
                "subSeries": dataset_record.get('subSeries'),
                "crossRefCompleteUrl": cross_ref_complete_url,
                "dataProvider": data_provider
            }
            htp_datasets.append(htp_dataset)

            if counter == batch_size:
                yield [htp_datasets,
                       dataset_tags,
                       publications,
                       cross_reference_list,
                       secondaryIds
                       ]
                counter = 0
                htp_datasets = []
                dataset_tags = []
                publications = []
                cross_reference_list = []
                secondaryIds = []

        if counter > 0:
            yield [htp_datasets,
                   dataset_tags,
                   publications,
                   cross_reference_list,
                   secondaryIds
                   ]


