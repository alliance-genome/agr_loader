"""Resource Descriptor Helper 2"""

import logging
import sys
import re
import yaml
from files import Download


class ResourceDescriptorHelper2():
    """Resource Descriptor Helper 2"""


    logger = logging.getLogger(__name__)


    def __init__(self):

        # TODO This should eventually be tied to the schemas submodule.
        url = 'https://raw.githubusercontent.com/' \
               + 'alliance-genome/agr_schemas/master/resourceDescriptors.yaml'

        resource_descriptor_file = Download('tmp',
                                            url,
                                            'resourceDescriptors.yaml').get_downloaded_data()

        self.yaml_list = yaml.load(resource_descriptor_file, Loader=yaml.SafeLoader)

        # Convert the list into a more useful lookup dictionary keyed by db_prefix.
        self.resource_descriptor_dict = {}
        for item in self.yaml_list:
            name = item['db_prefix']
            self.resource_descriptor_dict[name] = item

        # Iterate through this new dictionary and convert page lists to dictionaries.
        # These are keyed by the page name.
        for entry in self.resource_descriptor_dict:
            if 'pages' in self.resource_descriptor_dict[entry]:  # If we have a pages list.
                self.resource_descriptor_dict[entry]['pages_temp'] = dict()
                for page_item in self.resource_descriptor_dict[entry]['pages']:
                    page_name = page_item['name']
                    self.resource_descriptor_dict[entry]['pages_temp'][page_name] = page_item
                del self.resource_descriptor_dict[entry]['pages'] # Remove the old list.
                # Rename the new dict with the same name as the old list. For clarity.
                self.resource_descriptor_dict[entry]['pages'] = \
                        self.resource_descriptor_dict[entry].pop('pages_temp')

        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(self.resource_descriptor_dict)
        # quit()

    @staticmethod
    def alter_prefixes_to_match_resource_yaml(entry):
        """Alter Prefixes To Match Resource YAML"""

        # We use database prefixes that are not all uppercase.
        # TODO
        # The following line will cause some to fail. This needs to be addressed.
        entry = entry.upper()

        # Occasionally, prefixes from incoming files do not line up with the Alliance YAML.
        # The following dictionary translates these entries into the appropriate prefixes.
        prefix_translation_dictionary = {
            'WORMBASE' : 'WB',
            'FLYBASE' : 'FB',
            'RCSB PDB' : 'RCSB_PDB',
        }

        prefix = None
        if prefix_translation_dictionary.get(entry):
            prefix = prefix_translation_dictionary[entry]
        else:
            prefix = entry

        return prefix


    def split_identifier(self, identifier):
        """Split Identifier"""

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
            self.logger.info('Fatal Error: Identifier does not contain \':\' or \'-\' characters.')
            self.logger.info('Splitting identifier is not possible.')
            self.logger.info('Identifier: %s', identifier)
            sys.exit(-1)

        prefix = self.alter_prefixes_to_match_resource_yaml(prefix)

        return prefix, identifier_processed, separator

    def return_url(self, identifier, page):
        """Return URL"""

        db_prefix, identifier_stripped, separator = self.split_identifier(identifier)

        complete_url = None
        try:
            gid_pattern = self.resource_descriptor_dict[db_prefix]['gid_pattern']
            default_url = self.resource_descriptor_dict[db_prefix]['default_url']
        except KeyError:
            self.logger.info('Fatal Error: The database prefix \'{}\' '
                             'cannot be found in the Resource Descriptor YAML.'.format(db_prefix))
            self.logger.info('Page: %s', page)
            self.logger.info('Identifier: %s', identifier)
            sys.exit(-1)

        identifier_post_processed = db_prefix + separator + identifier_stripped

        regex_output = re.match(gid_pattern, identifier_post_processed)
        if regex_output is None:
            self.logger.info('Fatal Error: Cross Reference identifier did %s', \
                             'not match Resource Descriptor YAML file gid pattern.')
            self.logger.info('Database prefix: %s', db_prefix)
            self.logger.info('Identifier: %s', identifier_post_processed)
            self.logger.info('gid pattern: %s', gid_pattern)
            sys.exit(-1)
        if page is None and default_url is not None:
            complete_url = default_url.replace('[%s]', identifier_stripped)
        elif page is None and default_url is None:
            self.logger.info('Fatal Error: Cross Reference page is specified %s', \
                             'as None but the default url is not specified in the YAML.')
            self.logger.info('Database prefix: %s', db_prefix)
            self.logger.info('Identifier: %s', identifier_stripped)
            sys.exit(-1)
        elif page is not None:
            try:
                page_url = self.resource_descriptor_dict[db_prefix]['pages'][page]['url']
            except KeyError:
                self.logger.info('Fatal Error: The specified Cross Reference %s', \
                            'page or database prefix does not appear to exist.')
                self.logger.info('Database prefix: %s', (db_prefix))
                self.logger.info('Page: %s', page)
                self.logger.info('Identifier: %s', identifier_stripped)
                sys.exit(-1)
            complete_url = page_url.replace('[%s]', identifier_stripped)

        return complete_url
