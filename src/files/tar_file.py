"""TAR File"""

import logging
import os
import tarfile
import time


class TARFile():
    """TAR File"""

    logger = logging.getLogger(__name__)

    def __init__(self, path, tarfilename):
        self.path = path
        self.tarfilename = tarfilename

    def extract_all(self):
        """Extract All"""

        self.logger.debug("Reading header from %s/%s ...", self.path, self.tarfilename)

        members_to_extract = []
        extract = False

        attempts = 0
        # Our little retry loop. Implemented due to speed-related writing errors.
        # TODO Replace / update with "tenacity" module.
        while attempts < 3:
            try:
                tfile = tarfile.open(os.path.join(self.path, self.tarfilename), 'r')
                break
            except tarfile.ReadError as error:
                self.logger.warning('ReadError encountered when opening tar file.')
                self.logger.warning('Sleeping for 2 seconds and trying again.')
                self.logger.warning(error)
                attempts += 1
                time.sleep(2)
        if attempts == 3:
            raise tarfile.ReadError('Tar file could not be read after 3 attempts: %s + "/" + %s' \
                                    % (self.path, self.tarfilename))

        for member in tfile.getmembers():
            lower_name = member.name.lower()
            if 'gff' in lower_name:
                self.logger.info('Skipping GFF file extraction for %s', member.name)
                continue
            if os.path.exists(os.path.join(self.path, member.name)):
                self.logger.info('%s/%s already exists, not extracting.', self.path, member.name)
            else:
                self.logger.info("Extracting (%s->%s/%s)", member.name, self.path, member.name)
                members_to_extract.append(member)
                extract = True

        if extract is True:
            tfile.extractall(self.path, members_to_extract)
