import urllib.request
import codecs
import shutil
import os
import yaml
import uuid


class ResourceDescriptor:
    list_of_descriptor_maps_to_load = []

    def __init__(self, ):
        self.savepath = "schemas"
        self.filename = "resourceDescriptors.yaml"

    def get_data(self):
        if not os.path.exists(self.savepath):
            print("Making temp file storage: " + self.savepath)
            os.makedirs(self.savepath)
        url = "https://github.com/alliance-genome/agr_schemas/blob/develop/" + self.filename
        if not os.path.exists(self.savepath + "/" + self.filename):
            with urllib.request.urlopen(url) as response, open(self.savepath + "/" + self.filename, 'wb') as outfile:
                shutil.copyfileobj(response, outfile)
        else:
            print("File: " + self.savepath + "/" + self.filename + " already exists not downloading")

        with codecs.open(self.savepath + "/" + self.filename, 'r', 'utf-8') as stream:
            try:
                list_to_yield = []
                data = yaml.load(stream)
                for stanza in data:
                    pages = []
                    stanza_map = {}

                    resource = stanza.get("db_prefix")
                    pages = stanza.get("pages")
                    default_url = stanza.get("default_url")
                    gid_pattern = stanza.get("gid_pattern")
                    default_url_suffix = ""

                    if default_url is not None:
                        default_url_parts = default_url.split("[%s]")
                        default_url_prefix = default_url_parts[0]
                        if len(default_url_parts) > 1:
                            default_url_suffix = default_url_parts[1]

                    if pages is not None:
                        for page in pages:
                            page_url_suffix = ""
                            page_name = page.get("name")
                            page_url = page.get("url")
                            if page_url is not None:
                                page_url_parts = page_url.split("[%s]")
                                page_url_prefix = page_url_parts[0]
                                if len(page_url_parts) > 1:
                                    page_url_suffix = page_url_parts[1]

                                stanza_map[resource+page_name] = {"resource": resource,
                                              "default_url": default_url,
                                              "gid_pattern": gid_pattern,
                                              "page_name": page_name,
                                              "page_url": page_url,
                                              "page_url_prefix": page_url_prefix,
                                              "page_url_suffix": page_url_suffix,
                                              "default_url_prefix": default_url_prefix,
                                              "default_url_suffix": default_url_suffix,
                                              "primaryKey": resource + page_name,
                                              "uuid": str(uuid.uuid4())}
                                self.list_of_descriptor_maps_to_load.append(stanza_map)
                    else:
                        stanza_map[resource+"default"] = {"resource": resource,
                                      "default_url": default_url,
                                      "gid_pattern": gid_pattern,
                                      "default_url_prefix": default_url_prefix,
                                      "default_url_suffix": default_url_suffix,
                                      "page_url": "",
                                      "page_name": "",
                                      "page_url_prefix": default_url_prefix,
                                      "page_url_suffix": default_url_suffix,
                                      "primaryKey": resource + "default",
                                      "uuid": str(uuid.uuid4())
                                      }
                        self.list_of_descriptor_maps_to_load.append(stanza_map)

            except yaml.YAMLError as exc:
                print (exc)

        return self.list_of_descriptor_maps_to_load
