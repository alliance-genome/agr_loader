from files import *
import logging, yaml, os, pprint, sys
from .data_type_config import DataTypeConfig
from .mod_config import ModConfig
from cerberus import Validator
from services import taxon_to_MOD

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

    def get_config(self, type_of_data):
        pass

    def dispatch_to_object(self):
        dispatch_dictionary = {
            'BGI': ModConfig,
        }

        for config_entry in self.config_data.keys():
            logger.info('Processing data type: %s' % (config_entry))
            print(config_entry)
            print(self.config_data[config_entry])
        
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
        
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.config_data)
        pp.pprint(self.submission_system_data)

        # Create our config objects, either "DataType" or "MOD".
        self.dispatch_to_object()

        # for data_object in self.submission_system_data['dataFiles']:
        #     print(data_object['dataType'])
        # quit()

        # More logic here to generate config object
        pass