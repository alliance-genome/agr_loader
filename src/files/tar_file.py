import logging
logger = logging.getLogger(__name__)

import os
import tarfile

class TARFile(object):

    def __init__(self, path, tarfilename):
        self.path = path
        self.tarfilename = tarfilename

    def extract_all(self):
        logger.info("Reading header from %s/%s ..." % (self.path, self.tarfilename))

        members_to_extract = []
        extract = False
        tfile = tarfile.open(self.path + "/" + self.tarfilename, 'r')
        for member in tfile.getmembers():
            lower_name = member.name.lower()
            if 'gff' in lower_name:
                logger.info('Skipping GFF file extraction for %s' % (member.name))
                continue
            if not os.path.exists(self.path + "/" + member.name):
                logger.info("Extracting (%s->%s/%s)" % (member.name, self.path, member.name))
                members_to_extract.append(member)
                extract = True
            else:
                logger.info('%s/%s already exists, not extracting.' % (self.path, member.name))
        if extract is True:
            tfile.extractall(self.path, members_to_extract)