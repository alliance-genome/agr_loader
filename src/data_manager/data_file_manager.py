from files import *
import logging, yaml, os, pprint, sys
from .data_type_config import DataTypeConfig
from cerberus import Validator
from services import get_MOD_from_taxon

logger = logging.getLogger(__name__)

class DataFileManager(object):
    
    def __init__(self, config_file_loc):
        logger.info('Loading config file: %s' % (config_file_loc))
        config_file = open(config_file_loc, 'r')
        self.config_data = yaml.load(config_file)

        validation_yaml_file_loc = os.path.abspath('src/config/validation.yml')
        logger.info('Loading validation schema: %s' % (validation_yaml_file_loc))
        validation_schema_file = open(validation_yaml_file_loc, 'r')
        self.validation_schema = yaml.load(validation_schema_file)

        # Loading a JSON blurb from a file as a placeholder for submission system query.
        mock_submission_system_file_loc = os.path.abspath('src/config/mock_submission_system.json')
        self.submission_system_data = JSONFile().get_data(mock_submission_system_file_loc)

        # List used for MOD and data type objects.
        self.master_data_dictionary = {}

        # Dictionary for transformed submission system data.
        self.transformed_submission_system_data = {}
        
    def get_config(self, data_type):
        return self.master_data_dictionary.get(data_type)

    def dispatch_to_object(self):
        for config_entry in self.config_data.keys():
            logger.info('Processing data type: %s' % (config_entry))

            try:
                data_from_submission_system = self.transformed_submission_system_data[config_entry]
            except KeyError:
                logger.critical('Data type \'%s\' requested from YAML could not be found via submission system API call.' % (config_entry))
                logger.critical('Exiting.')
                sys.exit(-1)

            if config_entry == 'schemaVersion':
                self.master_data_dictionary['schemaVersion'] = self.transformed_submission_system_data[config_entry]
            elif config_entry == 'releaseVersion':
                self.master_data_dictionary['releaseVersion'] = self.transformed_submission_system_data[config_entry]
            else: 
                # Add the data type object to our master dictionary filed under the config_entry.
                self.master_data_dictionary[config_entry] = DataTypeConfig(
                    config_entry, 
                    self.config_data[config_entry],
                    data_from_submission_system)

    def download_and_validate(self):
        logger.info('Beginning download and validation.')
        for entry in self.master_data_dictionary.keys():
            logger.info('Downloading %s data.' % entry)
            if isinstance(self.master_data_dictionary[entry], DataTypeConfig): # If we're dealing with an object.
                self.master_data_dictionary[entry].get_data()
    # TODO validation

    def process_config(self):
        validator = Validator(self.validation_schema)
        validation_results = validator.validate(self.config_data)

        if validation_results is True:
            logger.info('Config file validation successful.')
        else:
            logger.critical('Config file validation unsuccessful!')
            for field, values in validator.errors.items():
                for value in values: # May have more than one error per field.
                    message = field + ': ' + value
                    logger.critical(message)
            logger.critical('Exiting')
            sys.exit(-1)
        
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint('self.config_data')
        # pp.pprint(self.config_data)
        # print('-------------------------------------------------')
        # pp.pprint('self.submission_system_data')
        # pp.pprint(self.submission_system_data)
        # print('-------------------------------------------------')

        # Transform the submission system data to be "data-type centered".
        self.restructure_submission_system_data()

        # Create our config objects, either "DataType" or "MOD".
        self.dispatch_to_object()

    def restructure_submission_system_data(self):
        self.transformed_submission_system_data['releaseVersion'] = self.submission_system_data['releaseVersion']
        self.transformed_submission_system_data['schemaVersion'] = self.submission_system_data['schemaVersion']

        # entry['tempExtractedFile'] is temporary since the final submission system will send the proper filename.

        for entry in self.submission_system_data['dataFiles']:
            if 'taxonId' in entry:
                MOD = get_MOD_from_taxon(entry['taxonId'])
                if entry['dataType'] in self.transformed_submission_system_data:
                    self.transformed_submission_system_data[entry['dataType']].append(
                        [MOD, entry['path'], entry['tempExtractedFile']]
                    )
                else:
                    self.transformed_submission_system_data[entry['dataType']] = []
                    self.transformed_submission_system_data[entry['dataType']].append(
                        [MOD, entry['path'], entry['tempExtractedFile']]
                    )
            else: # If we're dealing with non-MOD data.
                if entry['dataType'] in self.transformed_submission_system_data:
                    self.transformed_submission_system_data[entry['dataType']].append(
                        [entry['subType'], entry['path'], entry['tempExtractedFile']]
                    )
                else:
                    self.transformed_submission_system_data[entry['dataType']] = []
                    self.transformed_submission_system_data[entry['dataType']].append(
                        [entry['subType'], entry['path'], entry['tempExtractedFile']]
                    )

        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint('self.transformed_submission_system_data')
        # pp.pprint(self.transformed_submission_system_data)
        # print('-------------------------------------------------')