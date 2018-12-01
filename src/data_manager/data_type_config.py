import logging
logger = logging.getLogger(__name__)

from files import S3File, TXTFile, TARFile, Download
import os, sys
from transactors import FileTransactor
from .sub_type_config import SubTypeConfig

class DataTypeConfig(object):

    def __init__(self, data_type, submission_system_data):
        self.data_type = data_type
        self.submission_system_data = submission_system_data

        # TODO These will be set by the config YAML.
        self.neo4j_commit_size = 2500
        self.generator_batch_size = 10000

        self.list_of_subtype_objects = []

    def get_data(self):

        path = 'tmp'

        # Create our subtype objects.
        for downloadable_item in self.submission_system_data:
            if downloadable_item[2] is not None:
                full_path_to_send = path + '/' + downloadable_item[2]
            else:
                full_path_to_send = None # If we don't have a path.

            sub_type = SubTypeConfig(
                self.data_type, 
                downloadable_item[0], 
                downloadable_item[1], 
                full_path_to_send)

            self.list_of_subtype_objects.append(sub_type)

            # Send it off to be queued and executed.
            FileTransactor.execute_transaction(sub_type)


    def running_etl(self):
        return True

    def get_neo4j_commit_size(self):
        return self.neo4j_commit_size

    def get_generator_batch_size(self):
        return self.generator_batch_size    

    def check_for_single(self):
        if len(self.list_of_subtype_objects) > 1:
            logger.critical('Called for single item in object containing multiple children.')
            logger.critical('Please check the function calling for this single item.')
            sys.exit(-1)
        else:
            pass

    def get_single_filepath(self):
        self.check_for_single()
        return self.list_of_subtype_objects[0].get_filepath()

    def get_sub_type_objects(self):
        return self.list_of_subtype_objects