from files import *
import sys
import re

class DoExt:
    @staticmethod
    def get_data():
        path = "tmp";
        S3File("mod-datadumps", "disease-ontology.obo", path).download()
        do_data = TXTFile(path + "/disease-ontology.obo").get_data()

        do_dataset = {}

        creating_term = None

        for line in do_data:
            line = line.strip()

            if line == "[Term]":
                creating_term = True
            elif line == '': # Skip blank lines
                continue
            elif creating_term:
                key = (line.split(":")[0]).strip()
                value = ("".join(":".join(line.split(":")[1:]))).strip()

                if key == "id":
                    creating_term = value
                    do_dataset[creating_term] = {"id": value}
                    do_dataset[creating_term]['do_genes'] = [] # Empty dictionaries to receive entries later.
                    do_dataset[creating_term]['do_species'] = []
                elif key == "name":
                    do_dataset[creating_term]['name'] = value
                else:
                    if key == "synonym":
                        if value.split(" ")[-2] == "EXACT":
                            value = (" ".join(value.split(" ")[:-2]))[1:-1]
                        else:
                            continue
                    if key == "def":
                        m = re.search('\"(.+)\"', value)
                        value = m.group(1)

                    if key in do_dataset[creating_term]:
                        do_dataset[creating_term][key].append(value)
                    else:
                        do_dataset[creating_term][key] = [value]

        return do_dataset