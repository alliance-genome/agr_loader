"""Download."""

import logging
import os
import urllib.request
from urllib.error import HTTPError, URLError
from .gzip_file import GZIPFile

class Download(object):
    """Download"""

    logger = logging.getLogger(__name__)

    def __init__(self, savepath, url_to_retieve, filename_to_save):
        """Initilaise object."""
        self.savepath = savepath
        self.url_to_retrieve = url_to_retieve
        self.filename_to_save = filename_to_save
        self.full_filepath = os.path.join(self.savepath, self.filename_to_save)

    def get_downloaded_data(self):
        """Get Download Data."""
        self.download_file()

        with open(self.full_filepath) as file_handle:
            data = file_handle.read()
        return data

    def download_file(self):
        self.logger.debug("is_data_downloaded: Downloading data from: %s to: %s", self.url_to_retrieve, self.full_filepath)

        if not os.path.exists(self.savepath):
            self.logger.debug("Making temp file storage: %s", self.savepath)
            os.makedirs(self.savepath)

        if not os.path.exists(os.path.dirname(self.full_filepath)):
            self.logger.debug("Making temp file storage: %s", os.path.dirname(self.full_filepath))
            os.makedirs(os.path.dirname(self.full_filepath))

        if os.path.exists(self.full_filepath):
            self.logger.info("File: %s already exists not downloading", self.full_filepath)
        else:
            self.logger.info("File: %s does NOT exists downloading", self.full_filepath)
            count = 0
            while count < 10:
                count = count + 1
                try:
                    self.logger.debug("Downloading data file %s from: %s", self.filename_to_save, self.url_to_retrieve)
                    if self.url_to_retrieve.endswith('gz'):
                        urllib.request.urlretrieve(self.url_to_retrieve, self.full_filepath + ".gz")
                        GZIPFile(self.full_filepath + ".gz").extract()
                    else:
                        urllib.request.urlretrieve(self.url_to_retrieve, self.full_filepath)
                except (HTTPError, URLError) as error:
                    self.logger.error(error.reason)
                    continue
                break
