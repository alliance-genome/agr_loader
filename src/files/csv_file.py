import codecs
import csv
import logging

from .comment_file import CommentFile


logger = logging.getLogger(__name__)

class CSVFile(object):

    def __init__(self, filename):
        self.filename = filename

    def get_data(self):
        logger.info("Loading csv data from %s ..." % (self.filename))
        with codecs.open(self.filename, 'r', 'utf-8') as f:
            reader = csv.reader(CommentFile(f), delimiter='\t')
            rows = []
            for row in reader:
                rows.append(row)
        f.close()    
        return rows

