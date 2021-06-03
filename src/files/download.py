"""Download."""

import logging
import os
import time
import urllib.request
from urllib.error import HTTPError, URLError
from .gzip_file import GZIPFile
from loader_common import ContextInfo

class Download(object):
    """Download"""

    logger = logging.getLogger(__name__)

    def __init__(self, savepath, url_to_retieve, filename_to_save):
        """Initialise object."""
        self.savepath = savepath
        self.url_to_retrieve = url_to_retieve
        self.filename_to_save = filename_to_save
        self.full_filepath = os.path.join(self.savepath, self.filename_to_save)
        self.context_info = ContextInfo()

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

        if os.path.exists(self.full_filepath) and self.context_info.env["REDOWNLOAD_FROM_FMS"] == False:
            self.logger.info("File: %s already exists, not downloading", self.full_filepath)
        else:
            self.logger.info("File: %s does NOT exist, downloading", self.full_filepath)
            retries = 10
            while retries > 0:
                retries -= 1
                try:
                    self.logger.debug("Downloading data file %s from: %s", self.filename_to_save, self.url_to_retrieve)
                    if self.url_to_retrieve.endswith('gz'):
                        urllib.request.urlretrieve(self.url_to_retrieve, self.full_filepath + ".gz")
                        GZIPFile(self.full_filepath + ".gz").extract()
                    else:
                        urllib.request.urlretrieve(self.url_to_retrieve, self.full_filepath)
                except (HTTPError, URLError) as error:
                    self.logger.error(error.reason)
                    if retries > 0:
                        self.logger.warn("Downloading data file from %s failed. Retrying %s more times.", self.url_to_retrieve, retries)
                        # Wait certain amount of time before retrying (connectivity issues
                        # can automatically resolve, but only after some time)
                        time.sleep(6)
                        continue
                    else:
                        self.logger.error("Downloading data file from %s failed.", self.url_to_retrieve)
                        raise
                break
