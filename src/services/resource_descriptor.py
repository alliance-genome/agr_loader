import yaml, re, sys
import pprint
from files import Download
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ResourceDescriptor(object):

    def __init__(self):

        # TODO This should eventually be tied to the schemas submodule.       
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

        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(self.resource_descriptor_dict)
        # quit()

    def alter_prefixes_to_match_resource_yaml(self, entry):

        entry = entry.upper()

        # Occasionally, prefixes from incoming files do not line up with the Alliance YAML.
        # The following dictionary translates these entries into the appropriate prefixes.
        prefix_translation_dictionary = {
            'WORMBASE' : 'WB',
            'FLYBASE' : 'FB'
        }

        if entry not in prefix_translation_dictionary:
            return entry
        else:
            return prefix_translation_dictionary[entry]

    def split_identifier(self, identifier):
        prefix = None
        identifier_processed = None
        separator = None
        
        if ':' in identifier:
            prefix, identifier_processed = identifier.split(':', 1) # Split on the first occurrence
            separator = ':'
        elif '-' in identifier:
            prefix, identifier_processed = identifier.split('-', 1)  # Split on the first occurrence
            separator = '-'
        else:
            logger.info('Fatal Error: Identifier does not contain \':\' or \'-\' characters.')
            logger.info('Splitting identifier is not possible.')
            logger.info('Identifier: %s' % (identifier))
            sys.exit(-1)

        prefix = self.alter_prefixes_to_match_resource_yaml(prefix)

        return prefix, identifier_processed, separator

    def return_url(self, identifier, page):

        db_prefix, identifier_stripped, separator = self.split_identifier(identifier)

        complete_url = None
        gid_pattern = self.resource_descriptor_dict[db_prefix]['gid_pattern']
        default_url = self.resource_descriptor_dict[db_prefix]['default_url']

        identifier_post_processed = db_prefix + separator + identifier_stripped

        regex_output = re.match(gid_pattern, identifier_post_processed)
        if regex_output is None:
            logger.info('Fatal Error: Cross Reference identifier did not match Resource Descriptor YAML file gid pattern.')
            logger.info('Database prefix: %s' % (db_prefix))
            logger.info('Identifier: %s' % (identifier_post_processed))
            logger.info('gid pattern: %s' % (gid_pattern))
            sys.exit(-1)
        if page is None and default_url is not None:
            complete_url = default_url.replace('[%s]', identifier_stripped)
        elif page is None and default_url is None:
            logger.info('Fatal Error: Cross Reference page is specified as None but the default url is not specified in the YAML.')
            logger.info('Database prefix: %s' % (db_prefix))
            logger.info('Identifier: %s' % (identifier_stripped))
            sys.exit(-1)
        elif page is not None:
            try:
                page_url = self.resource_descriptor_dict[db_prefix]['pages'][page]['url']
            except KeyError:
                logger.info('Fatal Error: The specified Cross Reference page or database prefix does not appear to exist.')
                logger.info('Database prefix: %s' % (db_prefix))
                logger.info('Page: %s' % (page))
                logger.info('Identifier: %s' % (identifier_stripped))
                sys.exit(-1)
            complete_url = page_url.replace('[%s]', identifier_stripped)

        return complete_url
