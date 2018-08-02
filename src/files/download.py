import urllib.request
import os

class Download(object):

    def __init__(self, savepath, urlToRetieve, filenameToSave):
        self.savepath = savepath
        self.urlToRetrieve = urlToRetieve
        self.filenameToSave = filenameToSave

    def get_downloaded_data(self):
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

    def download_file(self):
        if not os.path.exists(os.path.dirname(self.savepath + "/" + self.filenameToSave)):
            print("Making temp file storage: %s" % (self.savepath+ "/" + self.filenameToSave))
            os.makedirs(os.path.dirname(self.savepath + "/" + self.filenameToSave))
        if not os.path.exists(self.savepath + "/" + self.filenameToSave):
            urllib.request.urlretrieve(self.urlToRetrieve, self.savepath + "/" + self.filenameToSave)

        else:
            print("File: %s/%s already exists not downloading" % (self.savepath, self.filenameToSave))
        return self.savepath + "/" + self.filenameToSave

    def list_files(self):
        pass
