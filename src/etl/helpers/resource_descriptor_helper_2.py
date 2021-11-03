"""Resource Descriptor Helper 2."""

import logging
import re
import yaml
from files import Download


class ResourceDescriptorHelper2():
    """Resource Descriptor Helper 2."""

    logger = logging.getLogger(__name__)
    resource_descriptor_dict = {}
    #########################
    # any local caching here.
    #########################

    # species/db key lookups caches obtained from aliases
    # i.e. for RGD:- 10116 => RGD, NCBITaxon:10116 => RGD, Rno => RGD , RGD => RGD
    key_lookup = {}

    # species short cuts
    key_to_fullname = {}
    key_to_shortname = {}
    key_to_order = {}
    key_to_taxonid = {}

    # report deprecated methods only one
    deprecated_mess = {}

    # missing pages
    missing_pages = {}

    # missing keys
    missing_keys = {}

    # bad_pages
    # can be deleted eventually being used to check old url whioch werte hardcoded
    # against new one for yaml.
    bad_pages = {}

    # identifier does not match the gid_pattern
    bad_regex = {}

    # DB should not generate a url.
    no_url = {}

    def get_key(self, alt_key, identifier=None):
        """Get species/DB main key.

        key_str: str to search for main key
        Try splitting to see if that helps if main lookup fails.
        If that also fails, try identifier lookup based on gid_pattern.
        """
        ret_key = None
        main_key = alt_key.upper()
        if main_key in self.key_lookup:
            ret_key = self.key_lookup[main_key]
            return ret_key

        # try split incase RGD:123456 or something passed
        key_prefix, _, _ = self.split_identifier(main_key, ignore_error=True)
        if key_prefix and key_prefix in self.key_lookup:
            self.logger.debug("%s Found after splitting", alt_key)
            ret_key = self.key_lookup[key_prefix]
            return ret_key

        #Try finding the correct match by matching the full identifier to the gid_pattern
        if identifier:
            for key in self.resource_descriptor_dict:
                gid_pattern = self.resource_descriptor_dict[key]['gid_pattern']

                if re.match(gid_pattern, identifier, re.IGNORECASE):
                    ret_key = self.key_lookup[key]
                    return ret_key

        #No options left, report failure to find a match
        if main_key in self.missing_keys:
            self.missing_keys[main_key] += 1
        else:
            self.missing_keys[main_key] = 1
            mess = "The database key '{}' --> '{}' cannot be found in the lookup.".format(alt_key, main_key)
            self.logger.critical(mess)
            self.logger.info("Available are %s", self.key_lookup.keys())

        return None

    def get_short_name(self, alt_key):
        """Get short name."""
        name = 'Alliance'
        try:
            key = self.get_key(alt_key)
            name = self.key_to_shortname[key]
        except KeyError:
            pass
        return name

    def get_full_name_from_key(self, alt_key):
        """Lookup fullname for a given species key.

        If key not found return None and let user deal with it.
        """
        key = self.get_key(alt_key)
        if key in self.key_to_fullname:
            return self.key_to_fullname[key]
        return None

    def get_taxon_from_key(self, alt_key):
        """Get taxon id (number bit only ) from key."""
        key = self.get_key(alt_key)
        if key in self.key_to_taxonid:
            return self.key_to_taxonid[key]
        return None

    def get_order(self, identifier):
        """Get order for a key."""
        order = None
        try:
            order = self.key_to_order[self.get_key(identifier)]
        except KeyError:
            self.logger.critical("Could not find orddr for identifier %s", identifier)
        return order

    def _get_alt_keys(self):
        """Get alternative keys for species.

        These are stored in the resourceDescriptor.yaml file under
        aliases. The keys for this are not used/stored but are here for reference
        or may be used at a later point.
        """
        url = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/ingest/species/species.yaml'
        self.logger.critical("species url is %s", url)

        resource_descriptor_file = Download('tmp', url, 'species.yaml').get_downloaded_data()

        yaml_list = yaml.load(resource_descriptor_file, Loader=yaml.SafeLoader)
        for item in yaml_list:
            db_name = item['primaryDataProvider']['dataProviderShortName'].upper()
            # Hack human data comes from RGD but we do not want to overwrite RGD
            # So hardcode test here to HGNC as the key instead.
            if db_name == 'RGD' and item['fullName'] == 'Homo sapiens':
                db_name = 'HUMAN'
                self.key_lookup['HUMAN'] = db_name
            self.key_lookup[db_name] = db_name
            self.key_lookup[item['fullName'].upper()] = db_name
            self.key_to_fullname[db_name] = item['fullName']
            for name in item['commonNames']:
                self.key_lookup[name.upper()] = db_name
            tax_word, tax_id, _ = self.split_identifier(item['taxonId'])
            self.key_to_taxonid[db_name] = tax_id
            self.key_lookup[item['taxonId'].upper()] = db_name
            self.key_lookup[tax_id] = db_name
            # Sce has 2 taxon id's so hard code the second one not in species file
            if item['fullName'] == 'Saccharomyces cerevisiae':
                self.key_lookup['4932'] = db_name
                self.key_lookup['NCBITAXON:4932'] = db_name
            self.key_lookup[item['shortName'].upper()] = db_name
            self.key_to_order[db_name] = item['phylogenicOrder']
            self.key_to_shortname[db_name] = item['shortName']

    def get_data(self):
        """Return dict."""
        return self.resource_descriptor_dict

    def __init__(self):
        """Load the dict from file."""
        if self.resource_descriptor_dict:
            self.logger.critical("keys are:- %s", self.resource_descriptor_dict.keys())
            return

        url = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/resourceDescriptors.yaml'

        resource_descriptor_file = Download('tmp', url, 'resourceDescriptors.yaml').get_downloaded_data()

        yaml_list = yaml.load(resource_descriptor_file, Loader=yaml.SafeLoader)
        # Convert the list into a more useful lookup dictionary keyed by db_prefix and aliases.
        resource_descriptor_dict = {}
        for item in yaml_list:
            main_key = item['db_prefix'].upper()
            resource_descriptor_dict[main_key] = item
            self.key_lookup[item['db_prefix']] = main_key
            self.key_lookup[main_key] = main_key
            self.key_lookup[item['name'].upper()] = main_key
            if 'aliases' in item:
                for alt_name in item['aliases']:
                    self.key_lookup[alt_name.upper()] = main_key
            if 'ignore_url_generation' in item:
                self.no_url[main_key] = 1
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

        ResourceDescriptorHelper2.resource_descriptor_dict = resource_descriptor_dict
        self._get_alt_keys()

    def split_identifier(self, identifier, ignore_error=False):
        """Split Identifier.

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
            if not ignore_error:
                key = "Identifier problem"
                if key not in self.missing_keys:
                    self.logger.critical('Identifier does not contain \':\' or \'-\' characters.')
                    self.logger.critical('Splitting identifier is not possible.')
                    self.logger.critical('Identifier: %s', identifier)
                    self.missing_keys[key] = 1
                else:
                    self.missing_keys[key] += 1
            prefix = identifier_processed = separator = None

        return prefix, identifier_processed, separator

    def missing_key_message(self, alt_key, key, value):
        """Generate message for missing key."""
        mk_key = "{}-{}".format(alt_key, key)
        if mk_key in self.missing_keys:
            self.missing_keys[mk_key] += 1
        else:
            self.missing_keys[mk_key] = 1
            mess = "The database prefix '{}' '{}' cannot be found in the Resource Descriptor YAML.".format(alt_key, key)
            self.logger.warning(mess)
            self.logger.warning('Identifier: %s', value)
            self.logger.info("keys are:- %s", self.key_lookup.keys())

    def return_url_from_key_value(self, prefix, local_id, alt_page=None):
        """Return url for a key value pair.

        prefix:   DB key i.e.             RGD,     MGI,    HGNC etc
        local_id: DB value i.e.           1311419, 80863,  33510
        alt_page:  page to get i.e.    gene,    allele, disease/human

        By default, if alt_page is not set it will use the main one 'default_url'
        """
        url = None
        key = self.get_key(prefix)
        if key in self.no_url:
            return ''
        if key not in self.key_lookup:
            self.missing_key_message(prefix, key, local_id)
            return None
        if 'default_url' not in self.resource_descriptor_dict[key]:
            mess = "******** '{}' has no 'default_url' **********".format(key)
            self.logger.warning(mess)
            self.logger.warning('Identifier: %s', local_id)
            return None
        try:
            if alt_page:
                page = alt_page
                url = self.resource_descriptor_dict[key]['pages'][alt_page]['url'].replace('[%s]', local_id.strip())
            else:
                page = 'default_url'
                url = self.resource_descriptor_dict[key]['default_url'].replace('[%s]', local_id.strip())
        except KeyError:
            mess = "{} does not exist for '{}' in the Resource Descriptor YAML.".format(page, key)
            key = "{}-{}".format(key, page)
            if key in self.missing_pages:
                self.missing_pages[key] += 1
            else:
                self.missing_pages[key] = 1
                self.logger.warning(mess)
        except AttributeError as e:
            mess = "***** ERROR!!! key = '{}', value = '{}' page = {} error = '{}'******".format(key, local_id, page, e)
            key = "{}-{}".format(key, page)
            if key in self.missing_pages:
                self.missing_pages[key] += 1
            else:
                self.missing_pages[key] = 1
                self.logger.warning(mess)
        return url

    def return_url(self, identifier, page):
        """Give message and call new one.

        Deprecated so give message but continue by calling new method.
        """
        if 'return_url' not in self.deprecated_mess:
            self.logger.info("return_url is Deprecated please use return_url_from_identifier")
            self.deprecated_mess['return_url'] = 1
        else:
            self.deprecated_mess['return_url'] += 1
        return self.return_url_from_identifier(identifier, page)

    def return_url_from_identifier(self, identifier, page=None):
        """Return URL for an identifier."""
        db_prefix, identifier_stripped, separator = self.split_identifier(identifier)

        key = self.get_key(db_prefix, identifier)
        if not key:
            return None
        if key in self.no_url:
            return None
        try:
            gid_pattern = self.resource_descriptor_dict[key]['gid_pattern']
        except KeyError:
            if key not in self.missing_keys:
                self.logger.warning("The database prefix '%s' has no 'gid_pattern'.", db_prefix)
                self.logger.warning('Page: %s', page)
                self.logger.warning('Identifier: %s', identifier)
                self.missing_keys[key] = 1
            else:
                self.missing_keys[key] += 1
            return None

        identifier_post_processed = db_prefix + separator + identifier_stripped

        regex_output = re.match(gid_pattern, identifier_post_processed, re.IGNORECASE)
        if regex_output is None:
            if key not in self.bad_regex:
                self.logger.warning('Cross Reference identifier did %s',
                                     'not match Resource Descriptor YAML file gid pattern.')
                self.logger.warning('Database prefix: %s', db_prefix)
                self.logger.warning('Identifier: %s', identifier_post_processed)
                self.logger.warning('gid pattern: %s', gid_pattern)
                self.logger.warning('page: %s', page)
                self.bad_regex[key] = 1
            else:
                self.bad_regex[key] += 1
        return self.return_url_from_key_value(db_prefix, identifier_stripped, alt_page=page)
