from files import *
from itertools import islice, chain, tee

import re

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
                next_key = (next_line.split(":")[0]).strip()
                if next_key == "name":
                    next_value = ("".join(":".join(next_line.split(":")[1:]))).strip()
                else:
                    sys.exit("FATAL ERROR: Expected SO name not found for %s" % (key))
                so_dataset = {value : next_value}
                so_list.append(so_dataset)
        return so_list

    def get_current_next(self, the_list):
        current, next_item = tee(the_list, 2)
        next_item = chain(islice(next_item, 1, None), [None])
        return zip(current, next_item)

        # The following is no longer used (since we only need ID and name for now).
        # if line == "[Term]":
        #     creating_term = True
        # elif creating_term:
        #     key = (line.split(":")[0]).strip()
        #     value = ("".join(":".join(line.split(":")[1:]))).strip()

        #     if key == "id":
        #         creating_term = value
        #         so_dataset[creating_term] = {}
        #     else:
        #         if key == "synonym":
        #             if value.split(" ")[-2] == "EXACT":
        #                 value = (" ".join(value.split(" ")[:-2]))[1:-1]
        #             else:
        #                 continue
        #         if key == "def":
        #             m = re.search('\"(.+)\"', value)
        #             value = m.group(1)

        #         if key in so_dataset[creating_term]:
        #             so_dataset[creating_term][key].append(value)
        #         else:
        #             so_dataset[creating_term][key] = [value]