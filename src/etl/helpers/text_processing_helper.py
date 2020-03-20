'''Text Processing Helper'''

import re

class TextProcessingHelper():
    '''Text Processing Helper'''

    @staticmethod
    def cleanhtml(raw_html):
        '''Clean HTML'''

        cleanr = re.compile('</.*?>')
        cleantext = re.sub(cleanr, '>', raw_html)
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '<', cleantext)

        return cleantext
