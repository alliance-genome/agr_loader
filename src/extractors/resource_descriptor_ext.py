import urllib.request
import codecs
import shutil
import os
import yaml

class ResourceDescriptor():

    def __init__(self, ):
        self.savepath = "schemas"
        self.filename = "resourceDescriptors.yaml"

    def get_data(self):
        # if not os.path.exists(self.savepath):
        #     print("Making temp file storage: " + self.savepath)
        #     os.makedirs(self.savepath)
        # url = "https://github.com/alliance-genome/agr_schemas/blob/master/" + self.filename
        # if not os.path.exists(self.savepath + "/" + self.filename):
        #     with urllib.request.urlopen(url) as response, open(self.savepath + "/" + self.filename, 'wb') as outfile:
        #         shutil.copyfileobj(response, outfile)
        # else:
        #     print("File: " + self.savepath + "/" + self.filename + " already exists not downloading")

        with codecs.open(self.savepath + "/" + self.filename, 'r', 'utf-8') as stream:
            try:
                print(yaml.dump(yaml.load(stream)))
            except yaml.YAMLError as exc:
                print(exc)
