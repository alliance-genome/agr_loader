from .comment_file import CommentFile
import csv
import codecs
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

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
