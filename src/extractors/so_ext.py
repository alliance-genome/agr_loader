from files import S3File, TXTFile
from itertools import islice, chain, tee
import sys

class SOExt:

    def get_data(self):
        path = "tmp";
        S3File("mod-datadumps/data", "so.obo", path).download()
        so_data = TXTFile(path + "/so.obo").get_data()

        so_list = []

        for current_line, next_line in self.get_current_next(so_data):
            so_dataset = {}
            current_line = current_line.strip()
            key = (current_line.split(":")[0]).strip()
            if key == "id":
                value = ("".join(":".join(current_line.split(":")[1:]))).strip()
                if not value.startswith('SO'):
                    continue
                next_key = (next_line.split(":")[0]).strip()
                if next_key == "name":
                    next_value = ("".join(":".join(next_line.split(":")[1:]))).strip()
                else:
                    sys.exit("FATAL ERROR: Expected SO name not found for %s" % (key))
                so_dataset = {
                'id' : value,
                'name' : next_value
                }
                so_list.append(so_dataset)
        return so_list

    def get_current_next(self, the_list):
        current, next_item = tee(the_list, 2)
        next_item = chain(islice(next_item, 1, None), [None])
        return zip(current, next_item)