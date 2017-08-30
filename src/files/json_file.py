import json
import codecs

class JSONFile(object):

    def get_data(self, filename):
        print("Loading json data from %s ..." % (filename))
        with codecs.open(filename, 'r', 'utf-8') as f:
            data = json.load(f)
        f.close()
        return data