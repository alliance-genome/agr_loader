import urllib.request
import os

class Download(object):

    def __init__(self, savepath, urlToRetieve, filenameToSave):
        self.savepath = savepath
        self.urlToRetrieve = urlToRetieve
        self.filenameToSave = filenameToSave

    def download(self):
        print("Downloading data from NCBI efetch ..." % (self.savepath, self.url))
        if not os.path.exists(self.savepath):
            print("Making temp file storage: %s" % (self.savepath))
            os.makedirs(self.savepath)
        if not os.path.exists(self.savepath + "/" + self.filenameToSave):
            urllib.request.urlretrieve(self.urlToRetrieve, self.savepath + "/" + self.filenameToSave)
        else:
            print("File: %s/%s already exists not downloading" % (self.savepath, self.filenameToSave))
        return self.savepath + "/" + self.filenameToSave

    def list_files(self):
        pass