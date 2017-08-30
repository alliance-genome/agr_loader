import urllib 
import os

class FTPFile(object):

    def __init__(self, url, savepath, filename):
        self.url = url
        self.savepath = savepath
        self.filename = filename

    def download(self):
        print("Downloading data from ftp %s->%s/%s ..." % (self.url, self.savepath, self.filename))
        if not os.path.exists(self.savepath):
            os.makedirs(self.savepath)
        urllib.urlretrieve(self.url, self.savepath + "/" + self.filename)