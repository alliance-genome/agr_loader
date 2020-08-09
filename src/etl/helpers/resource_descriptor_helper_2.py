"""Resource Descriptor Helper 2"""

import logging
import sys
import re
import yaml
import pprint
from files import Download


class ResourceDescriptorHelper2():
    """Resource Descriptor Helper 2"""

    logger = logging.getLogger(__name__)
    resource_descriptor_dict = {}
    #########################
    # any local caching here.
    #########################

    # species/db key lookups caches obtained from aliases
    # i.e. for RGD:- 10116 => RGD, NCBITaxon:10116 => RGD, Rno => RGD , RGD => RGD
    key_lookup = {}

    # species
    # i.e. RGD => 'Rattus norvegicus'
    key_to_fullname = {}

    # order
    # i.e. RGD => 20
    key_to_order = {}

    # report deprecated methids only one
    deprecated_mess = {}

    # missing pages
    missing_pages = {}

    # missing keys
    missing_keys = {}

    print("CRITICAL: BOB: initialise")

    def get_key(self, alt_key):
        """Get species/DB main key.

        key_str: str to search for main key
        Try splitting to see if that helps if main lookup fails.
        """
        ret_key = None
        alt_key = alt_key.upper()
        if alt_key not in self.key_lookup:
            # try split incase RGD:123456 or something passed
            key_prefix, _, _ = self.split_identifier(alt_key)
            if not key_prefix:
                return ret_key
            if key_prefix not in self.key_lookup:
                self.logger.debug("{} Found after splitting".format(alt_key))
                ret_key = self.key_lookup[key_prefix]
            else:
                if alt_key in self.missing_pages:
                    self.missing_pages[alt_key] += 1
                else:
                    self.missing_pages[alt_key] = 1
        else:
            ret_key = self.key_lookup[alt_key]
        return ret_key

    def get_full_name_from_key(self, key):
        """Lookup fullname for a given species key.

        If key not found return None and ket user deal with it.
        """
        key = self.get_key(key)
        if key in self.key_to_fullname:
            return self.key_to_fullname[key]
        return None

    def get_order(self, identifier):
        """Get order for a key."""
        order = None
        try:
            order = self.key_to_order[self.get_key(identifier)]
        except KeyError:
            self.logger.critical("Could not find orddr for identifier {}".format(identifier))
        return order

    def _get_alt_keys(self):
        """Get alternative keys for species.

        These are stored in the resourceDescriptor.yaml file under
        aliases. The keys for this are not used/stired but are here for reference
        or may be used at a later point.
        """
        url = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/ingest/species/species.yaml'
        resource_descriptor_file = Download('tmp',
                                            url,
                                            'species.yaml').get_downloaded_data()

        yaml_list = yaml.load(resource_descriptor_file, Loader=yaml.SafeLoader)
        pp = pprint.PrettyPrinter(indent=4)
        for item in yaml_list:
            db_name = item['primaryDataProvider']['dataProviderShortName'].upper()
            # Hack human data comes from RGD but we do not want to overwrite RGD
            # So hardcode test here to HGNC as the key instead.
            if db_name == 'RGD' and item['fullName'] == 'Homo sapiens':
                db_name = 'HGNC'
                self.key_lookup['HUMAN'] = db_name
            self.key_lookup[db_name] = db_name
            self.key_lookup[item['fullName'].upper()] = db_name
            self.key_to_fullname[db_name] = item['fullName']
            pp.pprint(item)
            for name in item['commonNames']:
                self.key_lookup[name.upper()] = db_name
            tax_word, tax_id, _ = self.split_identifier(item['taxonId'])
            self.key_lookup[item['taxonId'].upper()] = db_name
            self.key_lookup[tax_id] = db_name
            self.key_lookup[item['shortName'].upper()] = db_name
            self.key_to_order[db_name] = item['phylogenicOrder']

        pp.pprint(self.key_lookup)
        pp.pprint(self.key_to_fullname)

    def get_data(self):
        """Return dict."""
        return self.resource_descriptor_dict

    def __init__(self):
        """Load the dict from file."""
        if ResourceDescriptorHelper2.resource_descriptor_dict:
            self.logger.critical("BOB: Data already loaded returning")
            return
        # TODO This should eventually be tied to the schemas submodule.
        # NOTE: BOB change AGR-1144 to master once merged.
        url = 'https://raw.githubusercontent.com/' \
            + 'alliance-genome/agr_schemas/AGR-1144/resourceDescriptors.yaml'

        resource_descriptor_file = Download('tmp2',
                                            url,
                                            'resourceDescriptors.yaml').get_downloaded_data()

        yaml_list = yaml.load(resource_descriptor_file, Loader=yaml.SafeLoader)
        self.logger.critical("BOB: url = {}".format(url))
        self.logger.critical("BOB. H2 loading RD yaml")
        # Convert the list into a more useful lookup dictionary keyed by db_prefix.
        resource_descriptor_dict = {}
        for item in yaml_list:
            name = item['db_prefix']
            resource_descriptor_dict[name] = item
            if 'aliases' in item:
                print("BOB: {}".format(item['aliases']))
                for alt_name in item['aliases']:
                    print("BOB: alt_name is {}".format(alt_name))
                    self.key_lookup[alt_name.upper()] = item['db_prefix']

        # Iterate through this new dictionary and convert page lists to dictionaries.
        # These are keyed by the page name.
        for entry in resource_descriptor_dict:
            if 'pages' in resource_descriptor_dict[entry]:  # If we have a pages list.
                resource_descriptor_dict[entry]['pages_temp'] = dict()
                for page_item in resource_descriptor_dict[entry]['pages']:
                    page_name = page_item['name']
                    resource_descriptor_dict[entry]['pages_temp'][page_name] = page_item
                del resource_descriptor_dict[entry]['pages']  # Remove the old list.
                # Rename the new dict with the same name as the old list. For clarity.
                resource_descriptor_dict[entry]['pages'] = \
                    resource_descriptor_dict[entry].pop('pages_temp')

        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(self.resource_descriptor_dict)
        # quit()
        ResourceDescriptorHelper2.resource_descriptor_dict = resource_descriptor_dict
        self._get_alt_keys()

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
            'WORMBASE': 'WB',
            'FLYBASE': 'FB',
            'RCSB PDB': 'RCSB_PDB',
        }

        prefix = None
        if prefix_translation_dictionary.get(entry):
            prefix = prefix_translation_dictionary[entry]
        else:
            prefix = entry

        return prefix

    def split_identifier(self, identifier):
        """Split Identifier

        Does not throw exception anymore. Check return, if None returned, there was an error
        """

        prefix = None
        identifier_processed = None
        separator = None

        if ':' in identifier:
            prefix, identifier_processed = identifier.split(':', 1)  # Split on the first occurrence
            separator = ':'
        elif '-' in identifier:
            prefix, identifier_processed = identifier.split('-', 1)  # Split on the first occurrence
            separator = '-'
        else:
            key = "Identifier problem"
            if key not in self.missing_keys:
                self.logger.critical('Identifier does not contain \':\' or \'-\' characters.')
                self.logger.critical('Splitting identifier is not possible.')
                self.logger.critical('Identifier: %s', identifier)
                self.missing_keys[key] = 1
            else:
                self.missing_keys[key] += 1
            prefix = identifier_processed = separator = None
        if prefix:
            prefix = self.alter_prefixes_to_match_resource_yaml(prefix)

        return prefix, identifier_processed, separator

    def return_url_from_key_value(self, alt_key, value, alt_page=None):
        """Return url for a key value pair.

        key:   DB/Species key i.e.     RGD,     MGI,    HGNC etc
        value: DB/Species value i.e.   1311419, 80863,  33510
        alt_page:  page to get i.e.    gene,    allele, disease/human

        By default, if alt_page is not set it will use the main one 'default_url'
        """
        url = None
        key = self.get_key(alt_key)
        if key not in self.resource_descriptor_dict:
            mk_key = "{}-{}".format(alt_key, key)
            if mk_key in self.missing_keys:
                self.missing_keys[mk_key] += 1
            else:
                self.missing_keys[mk_key] = 1
                mess = "The database/species prefix '{}' '{}' cannot be found in the Resource Descriptor YAML.".format(alt_key, key)
                self.logger.critical(mess)
                self.logger.critical('Identifier: %s', value)
            return None
        if 'default_url' not in self.resource_descriptor_dict[key]:
            mess = "{} has no 'default_url'".format(key)
            self.logger.critical(mess)
            self.logger.critical('Identifier: %s', value)
            return None
        if not alt_page:
            try:
                url = self.resource_descriptor_dict[key]['default_url'].replace('[%s]', value.strip())
            except KeyError:
                mess = "default_url does not exist for '{}' in the Resource Descriptor YAML.".format(key)
                key = "{}-default_url".format(key)
                if key in self.missing_pages:
                    self.missing_pages[key] += 1
                else:
                    self.missing_pages[key] = 1
                self.logger.critical(mess)
                exit(-1)
            except AttributeError as e:
                mess = "ERROR!!! key = '{}', value = '{}' error = '{}'".format(key, value, e)
                key = "{}-default_url".format(key)
                if key in self.missing_pages:
                    self.missing_pages[key] += 1
                else:
                    self.missing_pages[key] = 1
                self.logger.critical(mess)
                exit(-1)
        else:
            try:
                url = self.resource_descriptor_dict[key]['pages'][alt_page]['url'].replace('[%s]', value.strip())
            except KeyError:
                key = "{}-{}".format(key, alt_page)
                if key in self.missing_pages:
                    self.missing_pages[key] += 1
                else:
                    mess = "page '{}' does not exist for '{}' in the Resource Descriptor YAML.".format(alt_page, key)
                    self.logger.critical(mess)
                    self.missing_pages[key] = 1
        return url

    def return_url(self, identifier, page):
        """Deprecated function please use return_url_from_identifier."""
        if 'return_url' not in self.deprecated_mess:
            self.logger.info("return_url is Deprecated please use return_url_from_identifier")
            self.deprecated_mess['return_url'] = 1
        else:
            self.deprecated_mess['return_url'] += 1
        return self.return_url_from_identifier(identifier, page)

    def return_url_from_identifier(self, identifier, page=None):
        """Return URL for an identifier."""
        db_prefix, identifier_stripped, separator = self.split_identifier(identifier)
        try:
            gid_pattern = self.resource_descriptor_dict[db_prefix]['gid_pattern']
        except KeyError:
            self.critical.info('The database prefix \'{}\' ',
                               'cannot be found in the Resource Descriptor YAML.'.format(db_prefix))
            self.logger.critical('Page: %s', page)
            self.logger.critical('Identifier: %s', identifier)
            sys.exit(-1)

        identifier_post_processed = db_prefix + separator + identifier_stripped

        regex_output = re.match(gid_pattern, identifier_post_processed)
        if regex_output is None:
            self.logger.critical('Cross Reference identifier did %s',
                                 'not match Resource Descriptor YAML file gid pattern.')
            self.logger.critical('Database prefix: %s', db_prefix)
            self.logger.critical('Identifier: %s', identifier_post_processed)
            self.logger.critical('gid pattern: %s', gid_pattern)
            sys.exit(-1)
        return self.return_url_from_key_value(db_prefix, identifier_stripped, alt_page=page)
