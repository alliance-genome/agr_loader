import logging
import os
import urllib.request


logger = logging.getLogger(__name__)

class Download(object):

    def __init__(self, savepath, urlToRetieve, filenameToSave):
        self.savepath = savepath
        self.urlToRetrieve = urlToRetieve
        self.filenameToSave = filenameToSave

    def get_downloaded_data(self):
        logger.info("Downloading data from ..." + self.urlToRetrieve)
        if not os.path.exists(self.savepath):
            logger.info("Making temp file storage: %s" % (self.savepath))
            os.makedirs(self.savepath)
        if not os.path.exists(self.savepath + "/" + self.filenameToSave):
            file = urllib.request.urlopen(self.urlToRetrieve)
            data = file.read()
            # retry the retrieval
            if data is None:
                file = urllib.request.urlopen(self.urlToRetrieve)
                data = file.read()
            file.close()
        else:
            logger.info("File: %s/%s already exists not downloading" % (self.savepath, self.filenameToSave))
        return data

    def download_file(self):
        if not os.path.exists(os.path.dirname(self.savepath + "/" + self.filenameToSave)):
            logger.info("Making temp file storage: %s" % (self.savepath+ "/" + self.filenameToSave))
            os.makedirs(os.path.dirname(self.savepath + "/" + self.filenameToSave))
        if not os.path.exists(self.savepath + "/" + self.filenameToSave):
            logger.info("Downloading data file %s from: %s" % (self.filenameToSave, self.urlToRetrieve))
            urllib.request.urlretrieve(self.urlToRetrieve, self.savepath + "/" + self.filenameToSave)

        else:
            logger.info("File: %s/%s already exists not downloading" % (self.savepath, self.filenameToSave))
        return self.savepath + "/" + self.filenameToSave

    def list_files(self):
        pass
