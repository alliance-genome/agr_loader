"""CateogryTag ETL."""

import logging
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class CategoryTagETL(ETL):
    """Category Tag ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    tag_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MERGE (e:CategoryTag {primaryKey: row.tag})
            ON CREATE SET e.tagName = row.tag,
                          e.tagDefinition = row.definition
               """

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

        self.logger.info("Loading HTP Tag Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        self.logger.info("Finished Loading HTP Tag Data: %s", sub_type.get_data_provider())
        if data is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
            return
        ETLHelper.load_release_info(data, sub_type, self.logger)

        commit_size = self.data_type_config.get_neo4j_commit_size()
        # batch_size = self.data_type_config.get_neo4j_commit_size()
        data_provider = sub_type.get_data_provider()
        self.logger.info("subtype: " + data_provider)

        query_template_list = [
                [self.tag_query_template, commit_size,
                 "tag_data_" + sub_type.get_data_provider() + ".csv"]
        ]

        generators = self.get_generators(data)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("CategoryTag-{}: ".format(sub_type.get_data_provider()))

    def get_generators(self, data):
        """Create Generator."""
        tag_maps = []

        for tag in data['data']:

            tag_object = {"tag": tag.get('Category'),
                          "name": tag.get('Category'),
                          "definition": tag.get('Definition')}
            tag_maps.append(tag_object)

        yield [tag_maps]
