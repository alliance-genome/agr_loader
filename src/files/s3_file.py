import logging
import os, time
import urllib.request
import sys

logger = logging.getLogger(__name__)

#TODO Consolidate these functions with download.py


class S3File(object):

    def __init__(self, filename, savepath):
        self.filename = filename
        self.savepath = savepath

    def download(self):
        if not os.path.exists(os.path.dirname(self.savepath + "/" + self.filename)):
            logger.debug("Making temp file storage: %s" % (self.savepath))
            os.makedirs(os.path.dirname(self.savepath + "/" + self.filename))
        url = "https://download.alliancegenome.org/" + self.filename
        if not os.path.exists(self.savepath + "/" + self.filename):
            logger.debug("Downloading data from s3 (https://download.alliancegenome.org/%s -> %s/%s) ..." % (self.filename, self.savepath, self.filename))
            urllib.request.urlretrieve(url, self.savepath + "/" + self.filename)
        else:
            logger.debug("File: %s/%s already exists, not downloading" % (self.savepath, self.filename))
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

        url = "https://download.alliancegenome.org/" + self.filename
        if not os.path.exists(self.savepath + "/" + self.filename):
            logger.info("Downloading data from s3 (https://download.alliancegenome.org/%s -> %s/%s) ..." % (self.filename, self.savepath, self.filename))
            urllib.request.urlretrieve(url, self.savepath + "/" + self.filename)
            return False
        else:
            logger.debug("File: %s/%s already exists, not downloading" % (self.savepath, self.filename))
            return True

    def list_files(self):
        pass
