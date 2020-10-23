import gzip
import shutil
import logging
import os


class GZIPFile(object):

    def __init__(self, path, filename):
        self.path = path
        self.filename = filename
        self.gzip_magic_number = '1f8b'
        self.logger = logging.getLogger(__name__)

    def extract(self):
        """Check whether the file is gzip encoded.
        If so, attempt to extract the data."""

        self.logger.info('Checking for gzip compression on file: {}'.format(self.filename))

        open_file = open(self.filename)
        if open_file.read(2).encode("hex") == self.gzip_magic_number:
            self.logger.info('Decompressing file: {}'.format(self.filename))
            output_filename = os.path.join(self.path, self.filename[:-3])
            with gzip.open(os.path.join(self.path, self.filename), 'rb') as f_in:
                with open(output_filename, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            self.logger.info('Skipping decompression on file: {}, no gzip magic number found.'
                             .format(self.filename))
