"""Gets the subtypes config"""

import logging
import os
import json
import sys

from pathlib import Path
from urllib.parse import urljoin
import jsonref
import jsonschema


from files import TARFile, Download, GZIPFile


class SubTypeConfig():
    """SubType Configuration"""

    logger = logging.getLogger(__name__)

    def __init__(self, data_type, sub_data_type, file_url, local_file_location):
        self.data_type = data_type
        self.sub_data_type = sub_data_type
        self.local_file_location = local_file_location
        self.file_url = file_url

        self.already_downloaded = False

    def get_filepath(self):
        """Get local_file_location"""

        return "tmp/" + str(self.local_file_location)

    def get_sub_data_type(self):
        """Get sub data type"""

        return self.sub_data_type

    def get_file_url(self):
        """Get file to download"""

        return self.file_url

    def get_data_provider(self):
        """Get data provider"""

        return self.sub_data_type

    def get_data(self):
        """get data"""

        # Grab the data (TODO validate).
        # Some of this algorithm is temporary.
        # e.g. Files from the submission system will arrive without the need for unzipping, etc.
        download_dir = 'tmp'
        self.logger.debug("URL: " + str(self.file_url))
        if self.local_file_location is not None and self.file_url is not None and  self.file_url.startswith('http'):
            Download(download_dir, self.file_url, self.local_file_location).download_file()
        else:
            self.logger.debug("File Path is None or not HTTP, not downloading")
            self.logger.debug("Not sure but doesn't happen? URL doesn't start with http -------------------------------------------------------")

