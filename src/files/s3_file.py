import logging
import os
import urllib.request


logger = logging.getLogger(__name__)

class S3File(object):

    def __init__(self, filename, savepath):
        self.filename = filename
        self.savepath = savepath

    def download(self):
        if not os.path.exists(os.path.dirname(self.savepath + "/" + self.filename)):
            logger.info("Making temp file storage: %s" % (self.savepath))
            os.makedirs(os.path.dirname(self.savepath + "/" + self.filename))
        url = "https://download.alliancegenome.org/" + self.filename
        if not os.path.exists(self.savepath + "/" + self.filename):
            logger.info("Downloading data from s3 (https://download.alliancegenome.org/%s -> %s/%s) ..." % (self.filename, self.savepath, self.filename))
            urllib.request.urlretrieve(url, self.savepath + "/" + self.filename)
        else:
            logger.info("File: %s/%s already exists, not downloading" % (self.savepath, self.filename))
        return self.savepath + "/" + self.filename

    def list_files(self):
        pass
