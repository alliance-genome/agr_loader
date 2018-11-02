from files import *
import logging, yaml
from cerberus import Validator
from services import taxon_to_MOD

logger = logging.getLogger(__name__)

class DataFileManager(object):
    
    def __init__(self, configfile):
        logger.info('Loading config file: %s' % (configfile))
        self.config_data = open(configfile, 'r')

        validation_yaml = 'config/validation.yml'
        logger.info('Loading validation schema: %s' % (validation_yaml))
        self.validation_schema = open(validation_yaml, 'r')

        # Loading a JSON blurb from a file as a placeholder for submission system query.
        self.mock_submission_system = 'config/mock_submission_system.json'
        logger.info('Loading mock submission system JSON: %s' % (self.mock_submission_system))

        # Dictionary used for storing config file information.
        self.config_dictionary = {}

    def taxon_to_mod(self, taxon):
        pass

    def download_and_validate(self):
        pass

    def process_config(self):
        validator = Validator(self.validation_schema)
        validation_results = validator.validate(self.config_data)

        if validation_results is True:
            logger.info('Config file validation successful.')
        else:
            logger.critical('Config file validation successful.')
            for field, values in validator.errors.items():
                for value in values: # May have more than one error per field.
                    message = field + ': ' + value
                    logger.critical(message)

        print(self.config_data)
        print(self.mock_submission_system)
        quit()

        # More logic here to generate config object
        pass