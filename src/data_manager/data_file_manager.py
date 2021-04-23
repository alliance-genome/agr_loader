"""Getting files from FMS"""

import logging
import os
import sys
import json
import yaml
import urllib3

from cerberus import Validator

from files import JSONFile
from loader_common import Singleton, ContextInfo
from urllib.parse import urlparse
from .data_type_config import DataTypeConfig


class DataFileManager(metaclass=Singleton):
    """Manages files"""

    logger = logging.getLogger(__name__)

    def __init__(self, config_file_loc):

        context_info = ContextInfo()

        # Load config yaml.
        self.logger.info('Loading config file: %s', config_file_loc)
        config_file = open(config_file_loc, 'r')
        self.config_data = yaml.load(config_file, Loader=yaml.SafeLoader)
        self.logger.debug("Config Data: %s", self.config_data)

        # Load validation yaml.
        validation_yaml_file_loc = os.path.abspath('src/config/validation.yml')
        self.logger.debug('Loading validation schema: %s', validation_yaml_file_loc)
        validation_schema_file = open(validation_yaml_file_loc, 'r')
        self.validation_schema = yaml.load(validation_schema_file, Loader=yaml.SafeLoader)

        # Assign values for thread counts.
        self.file_transactor_threads = self.config_data['FileTransactorThreads']
        self.neo4j_transactor_threads = self.config_data['Neo4jTransactorThreads']

        urllib3.disable_warnings()
        http = urllib3.PoolManager()

        # use the recently created snapshot
        api_url = context_info.env["FMS_API_URL"] + '/api/snapshot/release/' + context_info.env["ALLIANCE_RELEASE"]
        self.logger.info(api_url)

        submission_data = http.request('GET', api_url)

        if submission_data.status != 200:
            self.logger.error("Status: %s", submission_data.status)
            self.logger.error("No Data came from API: %s", api_url)
            sys.exit(-1)

        self.submission_system_data = json.loads(submission_data.data.decode('UTF-8'))

        # List used for MOD and data type objects.
        self.master_data_dictionary = {}

        # Dictionary for transformed submission system data.
        self.altered_submission_data = {}

        # process config file during initialization
        self.process_config()

    def get_file_transactor_thread_settings(self):
        """Gets File Transactors thread settings"""
        return self.file_transactor_threads

    def get_neo_transactor_thread_settings(self):
        """Gets NEO4J thread setting"""

        return self.neo4j_transactor_threads

    def get_config(self, data_type):
        """Get the object for a data type. If the object doesn't exist, this returns None."""

        self.logger.debug("Getting config for: [%s] -> Config[%s]",
                          data_type,
                          self.master_data_dictionary)
        return self.master_data_dictionary.get(data_type)

    def dispatch_to_object(self):
        """ This function sends off our data types to become DataTypeConfig objects.
            The smaller SubTypeConfig objects are created in the DataTypeConfig functions,
            see data_type_config.py."""

        for config_entry in self.altered_submission_data:
            # Skip string entries (e.g. schemaVersion, releaseVersion).
            if isinstance(self.altered_submission_data[config_entry], str):
                continue

            self.logger.debug('Processing DataType: %s', config_entry)

            # Create our data type object and add it to our master dictionary filed
            # under the config_entry.
            # e.g. Create BGI DataTypeConfig object and file it under BGI in the dictionary.
            self.master_data_dictionary[config_entry] = DataTypeConfig(config_entry,
                                                                       self.altered_submission_data[config_entry])

    def download_and_validate(self):
        """download and validate config file"""

        self.logger.debug('Beginning download and validation.')
        for entry in self.master_data_dictionary:
            self.logger.debug('Downloading %s data.', entry)
            if isinstance(self.master_data_dictionary[entry], DataTypeConfig):
                # If we're dealing with an object.
                self.master_data_dictionary[entry].get_data()
                self.logger.debug('done with %s data.', entry)

    def process_config(self):
        """ This checks for the validity of the YAML file.
             See src/config/validation.yml for the layout of the schema."""
        # TODO Add requirement checking and more validation to the YAML schema.

        validator = Validator(self.validation_schema)
        validation_results = validator.validate(self.config_data)

        if validation_results is True:
            self.logger.debug('Config file validation successful.')
        else:
            self.logger.critical('Config file validation unsuccessful!')
            for field, values in validator.errors.items():
                for value in values:  # May have more than one error per field.
                    message = field + ': ' + value
                    self.logger.critical(message)
            self.logger.critical('Exiting')
            sys.exit(-1)

        # Query the submission system for the required data.
        self.query_submission_system()

        # Create our DataTypeConfig (which in turn create our SubTypeConfig) objects.
        self.dispatch_to_object()

    def _search_submission_data(self, data_type, sub_type):

        # This function gets executed and sort-of fails for every non-data entry (like 'schemaVersion')
        # which are found in the config but not in the config_values_to_ignore array (query_submission_system fn)
        # TODO: implement solution. Options:
        # 1. Reorganize config file to define all datatypes inside a "data" (or similarly named) dictionary
        #     rather than as root-level key-value pairs. This enables iteration through this dictionary and voids the need for hardcoded exclusion lists.
        #     This is the preferred option (mluypaert).
        # 2. Extending config_values_to_ignore to include more keys.
        #     Using a hard-coded exclusion list is a hacky and error prone solution, as every time the config gets extended
        #     with new data types or configs, this exclusion list potentially need to be updated as well.
        #     This should be discouraged (mluypaert).
        try:
            returned_dict = next(item for item in self.submission_system_data['snapShot']['dataFiles']
                                 if item['dataType'].get('name') == data_type
                                 and item['dataSubType'].get('name') == sub_type)
        except StopIteration:
            self.logger.debug('dataType: %s subType: %s not found in submission system data.', data_type, sub_type)
            self.logger.debug('Creating entry with \'None\' path and extracted path.')
            returned_dict = {
                'dataType': data_type,
                'subType': sub_type,
                'path': None,
                'tempExtractedFile': None}

        return returned_dict

    def query_submission_system(self):
        """get file information from Submission System (FMS)"""

        # The list of tuples below is created to filter out submission
        # system data against our config file.
        ontologies_to_transform = ('GO', 'DOID', 'MI', 'ECOMAP')  # These have non-generic loaders.

        self.altered_submission_data['releaseVersion'] = \
            self.submission_system_data['snapShot']['releaseVersion']['releaseVersion']

        config_values_to_ignore = [
            'releaseVersion',  # Manually assigned above.
            'FileTransactorThreads',
            'Neo4jTransactorThreads']

        for entry in self.config_data.keys():  # Iterate through our config file.
            self.logger.debug("Entry: %s", entry)
            if entry not in config_values_to_ignore:  # Skip these entries.
                self.altered_submission_data[entry] = []  # Create our empty list.
                for sub_entry in self.config_data[entry]:
                    submission_system_dict = self._search_submission_data(entry, sub_entry)
                    path = submission_system_dict.get('s3Url')
                    self.logger.debug("Sub Entry: " + sub_entry)
                    self.logger.debug("submission_system_dict.get: %s", submission_system_dict)
                    temp_extracted_file = submission_system_dict.get('tempExtractedFile')
                    if temp_extracted_file is None or temp_extracted_file == '':
                        temp_extracted_file = urlparse(submission_system_dict.get('s3Url')).path[1:]
                        if temp_extracted_file is not None and len(temp_extracted_file) > 0 \
                                and temp_extracted_file.endswith('gz'):
                            temp_extracted_file = os.path.splitext(temp_extracted_file)[0]

                    # Special case for storing ontologies with non-generic loaders.
                    if sub_entry in ontologies_to_transform and entry == 'ONTOLOGY':
                        self.altered_submission_data[sub_entry] = []
                        self.altered_submission_data[sub_entry].append([sub_entry, path, temp_extracted_file])
                    else:
                        self.altered_submission_data[entry].append([sub_entry, path, temp_extracted_file])
            else:
                self.logger.debug("Ignoring entry: %s", entry)

        self.logger.debug("Loaded Types: %s", self.altered_submission_data)
