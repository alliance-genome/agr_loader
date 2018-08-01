import urllib.request
import os

class Download(object):

    def __init__(self, savepath, urlToRetieve, filenameToSave):
        self.savepath = savepath
        self.urlToRetrieve = urlToRetieve
        self.filenameToSave = filenameToSave

    def get_downloaded_file(self):
        print("Downloading data from ..." + self.urlToRetrieve)
        if not os.path.exists(self.savepath):
            print("Making temp file storage: %s" % (self.savepath))
            os.makedirs(self.savepath)
        if not os.path.exists(self.savepath + "/" + self.filenameToSave):
            file = urllib.request.urlopen(self.urlToRetrieve)
            data = file.read()
            file.close()
        else:
            print("File: %s/%s already exists not downloading" % (self.savepath, self.filenameToSave))
        return data

    def list_files(self):
        pass