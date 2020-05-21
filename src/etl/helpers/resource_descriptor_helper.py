'''Resources Descriptor Helper'''

import logging
import uuid
import yaml
from files import Download


class ResourceDescriptorHelper():
    '''Resource Descriptor Helper'''

    logger = logging.getLogger(__name__)
    list_of_descriptor_maps_to_load = []

    @staticmethod
    def get_data():
        '''Get Data'''

        ResourceDescriptorHelper.logger.info("got to resourcedescriptor")
        if len(ResourceDescriptorHelper.list_of_descriptor_maps_to_load) > 0:
            return ResourceDescriptorHelper.list_of_descriptor_maps_to_load

        url = 'https://raw.githubusercontent.com/'\
                + 'alliance-genome/agr_schemas/master/resourceDescriptors.yaml'
        resource_descriptor_file = Download('tmp',
                                            url,
                                            'resourceDescriptors.yaml').get_downloaded_data()

        yaml_list = yaml.load(resource_descriptor_file, Loader=yaml.SafeLoader)
        for stanza in yaml_list:
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
                        ResourceDescriptorHelper.list_of_descriptor_maps_to_load.append(stanza_map)

                        # TODO: fix special casing of NCBI links w/o pages in BGI
                        if resource == 'NCBI_Gene':
                            stanza_map[resource] = {"resource": resource,
                                                    "default_url": default_url,
                                                    "gid_pattern": gid_pattern,
                                                    "default_url_prefix": default_url_prefix,
                                                    "default_url_suffix": default_url_suffix,
                                                    "page_url": "",
                                                    "page_name": "",
                                                    "page_url_prefix": default_url_prefix,
                                                    "page_url_suffix": default_url_suffix,
                                                    "primaryKey": resource,
                                                    "uuid": str(uuid.uuid4())}

            else:
                stanza_map[resource] = {"resource": resource,
                                        "default_url": default_url,
                                        "gid_pattern": gid_pattern,
                                        "default_url_prefix": default_url_prefix,
                                        "default_url_suffix": default_url_suffix,
                                        "page_url": "",
                                        "page_name": "",
                                        "page_url_prefix": default_url_prefix,
                                        "page_url_suffix": default_url_suffix,
                                        "primaryKey": resource,
                                        "uuid": str(uuid.uuid4())}
                ResourceDescriptorHelper.list_of_descriptor_maps_to_load.append(stanza_map)

        return ResourceDescriptorHelper.list_of_descriptor_maps_to_load
