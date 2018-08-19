from files import Download
import yaml, re, sys
import pprint

class ResourceDescriptor(object):

    def __init__(self):       
        url = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/develop/resourceDescriptors.yaml'
        resource_descriptor_file = Download('tmp', url, 'resourceDescriptors.yaml').get_downloaded_data()

        self.yaml_list = yaml.load(resource_descriptor_file)
        
        # Convert the list into a more useful lookup dictionary keyed by db_prefix.
        self.resource_descriptor_dict = {}
        for item in self.yaml_list:
            name = item['db_prefix']
            self.resource_descriptor_dict[name] = item

        # Iterate through this new dictionary and convert page lists to dictionaries.
        # These are keyed by the page name.
        for entry in self.resource_descriptor_dict.keys():
            if 'pages' in self.resource_descriptor_dict[entry]:  # If we have a pages list.
                self.resource_descriptor_dict[entry]['pages_temp'] = dict() # Create a new dictionary
                for page_item in self.resource_descriptor_dict[entry]['pages']:
                    page_name = page_item['name']
                    self.resource_descriptor_dict[entry]['pages_temp'][page_name] = page_item
                del self.resource_descriptor_dict[entry]['pages'] # Remove the old list.
                # Rename the new dict with the same name as the old list. For clarity.
                self.resource_descriptor_dict[entry]['pages'] = self.resource_descriptor_dict[entry].pop('pages_temp')

    def return_url(self, db_prefix, identifier, page):

        complete_url = None
        gid_pattern = self.resource_descriptor_dict[db_prefix]['gid_pattern']
        default_url = self.resource_descriptor_dict[db_prefix]['default_url']

        regex_output = re.match(gid_pattern, identifier)
        if regex_output is None:
            print('Fatal Error: Cross Reference identifier did not match Resource Descriptor YAML file gid pattern.')
            print('Identifier: %s' % (identifier))
            print('gid pattern: %s' % (gid_pattern))
            sys.exit(-1)

        if page is None and default_url is not None:
            complete_url = default_url.replace('[%s]', identifier)
        elif page is None and default_url is None:
            print('Fatal Error: Cross Reference page is specified as None but the default url is not specified in the YAML.')
            print('Database prefix: %s' % (db_prefix))
            print('Identifier: %s' % (identifier))
            sys.exit(-1)
        elif page is not None:
            try:
                page_url = self.resource_descriptor_dict[db_prefix]['pages'][page]['url']
            except KeyError:
                print('Fatal Error: The specified Cross Reference page or database prefix does not appear to exist.')
                print('Database prefix: %s' % (db_prefix))
                print('Page: %s' % (page))
                print('Identifier: %s' % (identifier))
                sys.exit(-1)
            complete_url = page_url.replace('[%s]', identifier)

        return complete_url