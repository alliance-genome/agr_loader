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
              ds.subSeries = row.subSeries
         """

    htp_dataset_pub_query_template = """
        
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
        MATCH (ds:HTPDataset {primaryKey: row.datasetId})
        
        MERGE (p:Publication {primaryKey: row.pubPrimaryKey})
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

        MATCH (ds:HTPDataset {primaryKey: row.datasetId})
        
        MERGE (s:SecondaryId:Identifier {primaryKey:row.secondaryId})
                ON CREATE SET s.name = row.secondaryId
                
        MERGE (ds)-[aka:ALSO_KNOWN_AS]-(s)
   

    """

    htpdataset_xrefs_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (o:HTPDataset {primaryKey:row.datasetId}) """ + ETLHelper.get_cypher_xref_text()

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
             [HTPMetaDatasetETL.htp_dataset_query_template, commit_size,"htp_metadataset_" + sub_type.get_data_provider() + ".csv"],
             [HTPMetaDatasetETL.htp_category_tags_query_template, commit_size,"htp_metadataset_tags_" + sub_type.get_data_provider() + ".csv"],
             [HTPMetaDatasetETL.htp_dataset_pub_query_template, commit_size,"htp_metadataset_publications_" + sub_type.get_data_provider() + ".csv"],
             [HTPMetaDatasetETL.htpdataset_xrefs_template, commit_size, "htp_metadataset_xrefs_" + sub_type.get_data_provider() + ".csv"],
             [HTPMetaDatasetETL.htp_secondaryIds_query_template, commit_size,"htp_metadataset_secondaryIds_" + sub_type.get_data_provider() + ".csv"],

        ]

        # Obtain the generator
        generators = self.get_generators(data, batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, htp_dataset_data, batch_size):
        dataset_tags = []
        data_providers = []
        htp_datasets = []
        publications = []
        secondaryIds = []
        cross_reference_list = []
        counter = 0
        date_produced = htp_dataset_data['metaData']['dateProduced']

        data_provider_object = htp_dataset_data['metaData']['dataProvider']

        data_provider_cross_ref = data_provider_object.get('crossReference')
        data_provider = data_provider_cross_ref.get('id')
        data_provider_pages = data_provider_cross_ref.get('pages')
        data_provider_cross_ref_set = []


        if data_provider_pages is not None:
            for data_provider_page in data_provider_pages:
                cross_ref_complete_url = ETLHelper.get_page_complete_url(data_provider, self.xref_url_map, data_provider,
                                                                      data_provider_page)

                data_provider_cross_ref_set.append(ETLHelper.get_xref_dict(data_provider, data_provider, data_provider_page,
                                                                       data_provider_page, data_provider,
                                                                       cross_ref_complete_url,
                                                                       data_provider + data_provider_page))

                data_providers.append(data_provider)
                logger.info("data provider: " + data_provider)

        for dataset_record in htp_dataset_data['data']:

            counter = counter + 1

            dataset = dataset_record.get('datasetId')
            datasetId = dataset.get('primaryId')

            # spoke to RGD and they wish to remove these datasets as they overlap with SGD.

            if (datasetId == 'GEO:GSE18157' or datasetId=='GEO:GSE33497') and data_provider == 'RGD':
                continue
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

            if 'crossReference' in dataset:
                crossRefO = dataset.get('crossReference')
                if crossRefO is not None:
                    crossRefId = crossRefO.get('id')
                    local_crossref_id = crossRefId.split(":")[1]
                    prefix =crossRefId.split(":")[0]
                    pages = crossRefO.get('pages')

                    # some pages collection have 0 elements
                    if pages is not None and len(pages) > 0:
                        for page in pages:
                            mod_global_cross_ref_id = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                      self.xref_url_map, prefix, page)
                            xref = ETLHelper.get_xref_dict(local_crossref_id, prefix, page, page, crossRefId,
                                                               mod_global_cross_ref_id, crossRefId + page)
                            xref['dataId'] = datasetId
                            cross_reference_list.append(xref)

            category_tags = dataset_record.get('categoryTags')

            if category_tags is not None:
                for tag in category_tags :
                    dataset_category_tag = {
                        "datasetId": datasetId,
                        "tag": tag
                    }
                    dataset_tags.append(dataset_category_tag)

            publicationNew = dataset_record.get('publication')
            if publicationNew is not None:
                for pub in publicationNew:
                    pid = pub.get('publicationId')
                    publication_mod_id = ""
                    pub_med_id = ""
                    pub_mod_url = ""
                    pub_med_url = ""
                    if pid is not None and pid.startswith('PMID:'):
                        pub_med_id = pub.get('publicationId')
                        local_pub_med_id = pub_med_id.split(":")[1]
                        pub_med_url = ETLHelper.get_complete_pub_url(local_pub_med_id, pub_med_id)
                        if 'crossReference' in pub:
                            pub_xref = pub.get('crossReference')
                            publication_mod_id = pub_xref.get('id')
                            local_pub_mod_id = publication_mod_id.split(":")[1]
                            pub_mod_url = ETLHelper.get_complete_pub_url(local_pub_mod_id, publication_mod_id)
                    elif pid is not None and not pid.startswith('PMID:'):
                        publication_mod_id = pub.get('publicationId')
                        local_pub_mod_id = publication_mod_id.split(":")[1]
                        pub_mod_url = ETLHelper.get_complete_pub_url(local_pub_mod_id, publication_mod_id)

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
                "subSeries": dataset_record.get('subSeries')
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

