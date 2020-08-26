"""S3 File."""

import logging
import os
import time
import urllib.request
from loader_common import ContextInfo


class S3File():
    """S3 File."""

    logger = logging.getLogger(__name__)

    def __init__(self, filename, savepath):
        """Initialise object."""
        self.filename = filename
        self.savepath = savepath

        self.context_info = ContextInfo()
        self.download_url = "https://" + self.context_info.env["DOWNLOAD_HOST"] + "/" + self.filename

    def download(self):
        """Download."""
        if not os.path.exists(os.path.dirname(os.path.join(self.savepath, self.filename))):
            self.logger.info("Making temp file storage: %s", os.path.dirname(os.path.join(self.savepath, self.filename)))
            os.makedirs(os.path.dirname(os.path.join(self.savepath, self.filename)))

        url = self.download_url
        self.logger.info(url)
        if not os.path.exists(os.path.join(self.savepath, self.filename)):
            self.logger.info("Downloading data from s3 (https://%s/%s -> %s/%s) ...",
                             self.context_info.env["DOWNLOAD_HOST"],
                             self.filename,
                             self.savepath,
                             self.filename)
            urllib.request.urlretrieve(url, os.path.join(self.savepath, self.filename))
        else:
            self.logger.info("File: %s/%s already exists, not downloading",
                             self.savepath,
                             self.filename)
        return os.path.join(self.savepath, self.filename)

    def download_new(self):
        """Download New."""
        if not os.path.exists(os.path.dirname(os.path.join(self.savepath, self.filename))):
            self.logger.debug("Making temp file storage: %s", os.path.dirname(os.path.join(self.savepath, self.filename)))

            # Our little retry loop. Implemented due to speed-related writing errors.
            # TODO Replace / update with "tenacity" module.
            attempts = 0
            while attempts < 3:
                try:
                    os.makedirs(os.path.dirname(os.path.join(self.savepath, self.filename)))
                    break
                except FileExistsError:
                    # Occassionally, two processes can attempt to create the directory
                    # at almost the exact same time.
                    # This allows except should allow this condition to pass without issue.
                    break
                except OSError as error:
                    self.logger.warning('OSError encountered when creating directories.')
                    self.logger.warning('Sleeping for 2 seconds and trying again.')
                    self.logger.warning(error)
                    attempts += 1
                    time.sleep(2)
            if attempts == 3:
                raise OSError('Critical error downloading file (attempted 3 times): %s + "/" + %s' % (self.savepath, self.filename))

        url = self.download_url
        self.logger.info(url)
        self.logger.info("Checking for file %s", os.path.join(self.savepath, self.filename))
        if os.path.exists(os.path.join(self.savepath, self.filename)):
            self.logger.debug("File: %s/%s already exists, not downloading",
                              self.savepath,
                              self.filename)
            return True

        self.logger.debug("Downloading data from s3 (https://%s/%s -> %s/%s) ...",
                          self.context_info.env["DOWNLOAD_HOST"],
                          self.filename,
                          self.savepath,
                          self.filename)
        urllib.request.urlretrieve(url, os.path.join(self.savepath, self.filename))

        return False

    def list_files(self):
        """List Files."""
