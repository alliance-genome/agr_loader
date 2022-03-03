"""Gets information from YAML configs"""

import logging
import sys
import os

from transactors import FileTransactor
from .sub_type_config import SubTypeConfig


class DataTypeConfig():
    """Data Type Config"""
    logger = logging.getLogger(__name__)

    def __init__(self, data_type, submission_system_data):
        self.data_type = data_type
        self.submission_system_data = submission_system_data

        # TODO These will be set by the config YAML.
        self.neo4j_commit_size = 25000
        self.generator_batch_size = 10000

        self.list_of_subtype_objects = []

    def get_data(self):
        """Download data and put in tmp folder"""
        # Create our subtype objects.
        for downloadable_item in self.submission_system_data:
            sub_type = SubTypeConfig(self.data_type, downloadable_item[0], downloadable_item[1], downloadable_item[2])

            self.list_of_subtype_objects.append(sub_type)

            # Send it off to be queued and executed.
            FileTransactor.execute_transaction(sub_type)

    def get_neo4j_commit_size(self):
        """Returns NEO4J commit size"""

        return self.neo4j_commit_size

    def get_generator_batch_size(self):
        """Returns generator Batch size"""

        return self.generator_batch_size

    def check_for_single(self):
        """Determine if list of subtypes is only one"""

        if len(self.list_of_subtype_objects) > 1:
            self.logger.critical('Called for single item in object containing multiple children.')
            self.logger.critical('Please check the function calling for this single item.')
            sys.exit(-1)
        else:
            pass

    def get_single_filepath(self):
        """Gets filepath for single file"""

        self.check_for_single()
        return self.list_of_subtype_objects[0].get_filepath()

    def get_sub_type_objects(self):
        """Gets subtype objects"""

        return self.list_of_subtype_objects
