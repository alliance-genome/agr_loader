import logging
import os
import tarfile


logger = logging.getLogger(__name__)

class TARFile(object):

    def __init__(self, path, tarfilename):
        self.path = path
        self.tarfilename = tarfilename

    def extract_all(self):
        logger.info("Reading header from %s/%s ..." % (self.path, self.tarfilename))

        tfile = tarfile.open(self.path + "/" + self.tarfilename, 'r')
        extract = False
        for member in tfile.getmembers():
            if not os.path.exists(self.path + "/" + member.name):
                logger.info("Extracting (%s->%s/%s)" % (member.name, self.path, member.name))
                extract = True
            else:
                logger.info('%s/%s already exists, not extracting.' % (self.path, member.name))
        if extract is True:
            tfile.extractall(self.path)
