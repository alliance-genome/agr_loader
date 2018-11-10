from files import S3File, TXTFile, TARFile, Download
import os, logging

logger = logging.getLogger(__name__)

# TODO Make this a subclass of data_type_config.

class SubTypeConfig(object):

    def __init__(self, sub_data_type, filepath):
        self.sub_data_type = sub_data_type
        self.filepath = filepath

    def get_filepath(self):
        return self.filepath

    def get_data_provider(self):
        return self.sub_data_type