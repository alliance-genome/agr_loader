'''ZIP File'''

import logging
import os
import zipfile

class ZIPFile():
    '''ZIP File'''

    logger = logging.getLogger(__name__)

    def __init__(self, path, zipfilename):
        self.path = path
        self.zipfilename = zipfilename

    def extract_all(self):
        '''Extrall All'''

        self.logger.debug("Extracting file(s) from %s/%s ...",
                          self.path,
                          self.zipfilename)

        with zipfile.ZipFile(os.path.join(self.path, self.zipfilename), 'r') as zip_ref:
            zip_ref.extractall(os.path.dirname(os.path.join(self.path, self.zipfilename)))
