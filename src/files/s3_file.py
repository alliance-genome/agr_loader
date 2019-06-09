import logging
import os
import time
import urllib.request
from common import ContextInfo

logger = logging.getLogger(__name__)


class S3File(object):

    def __init__(self, filename, savepath):
        self.filename = filename
        self.savepath = savepath

        self.context_info = ContextInfo()
        self.download_url = "https://" + self.context_info.env["DOWNLOAD_HOST"] + "/" + self.filename

    def download(self):
        if not os.path.exists(os.path.dirname(self.savepath + "/" + self.filename)):
            logger.info("Making temp file storage: %s" % (self.savepath))
            os.makedirs(os.path.dirname(self.savepath + "/" + self.filename))

        url = self.download_url
        logger.info(url)
        if not os.path.exists(self.savepath + "/" + self.filename):
            logger.info("Downloading data from s3 (https://%s/%s -> %s/%s) ..." % (self.context_info.env["DOWNLOAD_HOST"], self.filename, self.savepath, self.filename))
            urllib.request.urlretrieve(url, self.savepath + "/" + self.filename)
        else:
            logger.info("File: %s/%s already exists, not downloading" % (self.savepath, self.filename))
        return self.savepath + "/" + self.filename

    def download_new(self):
        if not os.path.exists(os.path.dirname(self.savepath + "/" + self.filename)):
            logger.debug("Making temp file storage: %s" % (self.savepath))

            # Our little retry loop. Implemented due to speed-related writing errors.
            # TODO Replace / update with "tenacity" module.
            attempts = 0
            while attempts < 3:
                try: 
                    os.makedirs(os.path.dirname(self.savepath + "/" + self.filename))
                    break
                except FileExistsError:
                    # Occassionally, two processes can attempt to create the directory at almost the exact same time.
                    # This allows except should allow this condition to pass without issue.
                    break
                except OSError as e:
                    logger.warn('OSError encountered when creating directories.')
                    logger.warn('Sleeping for 2 seconds and trying again.')
                    logger.warn(e)
                    attempts += 1
                    time.sleep(2)
            if attempts == 3:
                raise OSError('Critical error downloading file (attempted 3 times): %s + "/" + %s' % (self.savepath, self.filename))

        url = self.download_url
        logger.info(url)
        if not os.path.exists(self.savepath + "/" + self.filename):
            logger.debug("Downloading data from s3 (https://%s/%s -> %s/%s) ..." % (self.context_info.env["DOWNLOAD_HOST"], self.filename, self.savepath, self.filename))
            urllib.request.urlretrieve(url, self.savepath + "/" + self.filename)
            return False
        else:
            logger.debug("File: %s/%s already exists, not downloading" % (self.savepath, self.filename))
            return True

    def list_files(self):
        pass
