"""Download"""

import logging


import os
import urllib.request
from urllib.error import HTTPError, URLError


class Download():
    """Download"""


    logger = logging.getLogger(__name__)


    def __init__(self, savepath, url_to_retieve, filename_to_save):
        self.savepath = savepath
        self.url_to_retrieve = url_to_retieve
        self.filename_to_save = filename_to_save


    def get_downloaded_data(self):
        """Get Download Data"""

        self.logger.critical("Downloading data from: %s to %s", self.url_to_retrieve, self.savepath)

        if not os.path.exists(self.savepath):
            self.logger.debug("Making temp file storage: %s", self.savepath)
            os.makedirs(self.savepath)

        full_filepath = os.path.join(self.savepath, self.filename_to_save)
        self.logger.critical("Downloading data to {}".format(full_filepath))
        if  os.path.exists(full_filepath):
            self.logger.info("File: %s already exists not downloading", full_filepath)
        else:
            count = 0
            while count < 10:
                count = count + 1
                try:
                    urllib.request.urlretrieve(self.url_to_retrieve, full_filepath)
                except (HTTPError, URLError) as error:
                    self.logger.error(error.reason)
                    continue
                break

        with open(full_filepath) as file_handle:
            data = file_handle.read()
        return data


    def is_data_downloaded(self):
        """Is Data Downloaded"""

        self.logger.info("Downloading data from: %s", self.url_to_retrieve)

        if not os.path.exists(self.savepath):
            self.logger.debug("Making temp file storage: %s", self.savepath)
            os.makedirs(self.savepath)

        if not os.path.exists(os.path.join(self.savepath, self.filename_to_save)):
            urllib.request.urlretrieve(self.url_to_retrieve,
                                       os.path.join(self.savepath,
                                                    self.filename_to_save))
            return False

        self.logger.info("File: %s/%s already exists",
                         self.savepath,
                         self.filename_to_save)
        return True


    def download_file(self):
        """Download File"""

        if not os.path.exists(os.path.dirname(os.path.join(self.savepath,
                                                           self.filename_to_save))):
            self.logger.info("Making temp file storage: %s",
                             os.path.join(self.savepath, self.filename_to_save))
            os.makedirs(os.path.dirname(os.path.join(self.savepath,
                                                     self.filename_to_save)))
        if not os.path.exists(os.path.join(self.savepath, self.filename_to_save)):
            self.logger.info("Downloading data file %s from: %s",
                             self.filename_to_save,
                             self.url_to_retrieve)
            urllib.request.urlretrieve(self.url_to_retrieve,
                                       os.path.join(self.savepath,
                                                    self.filename_to_save))

        else:
            self.logger.info("File: %s/%s already exists not downloading",
                             self.savepath,
                             self.filename_to_save)

        return os.path.join(self.savepath, self.filename_to_save)


    def list_files(self):
        """List files"""
