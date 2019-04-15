import logging, yaml, os, sys, json, urllib3, requests

from cerberus import Validator

from files import JSONFile
from etl.helpers import ETLHelper
from common import Singleton
from .data_type_config import DataTypeConfig

logger = logging.getLogger(__name__)


class DataFileManager(metaclass=Singleton):
    
    def __init__(self, config_file_loc):
        # Load config yaml.
        logger.info('Loading config file: %s' % config_file_loc)
        config_file = open(config_file_loc, 'r')
        self.config_data = yaml.load(config_file, Loader=yaml.SafeLoader)
        logger.debug("Config Data: %s" % self.config_data)

        # Load validation yaml.
        validation_yaml_file_loc = os.path.abspath('src/config/validation.yml')
        logger.info('Loading validation schema: %s' % validation_yaml_file_loc)
        validation_schema_file = open(validation_yaml_file_loc, 'r')
        self.validation_schema = yaml.load(validation_schema_file, Loader=yaml.SafeLoader)

        # Assign values for thread counts.
        self.FileTransactorThreads = self.config_data['FileTransactorThreads']
        self.Neo4jTransactorThreads = self.config_data['Neo4jTransactorThreads']

        # Loading a JSON blurb from a file as a placeholder for submission system query.
        other_file_meta_data = os.path.abspath('src/config/local_submission.json')
        self.non_submission_system_data = JSONFile().get_data(other_file_meta_data)
        urllib3.disable_warnings()
        http = urllib3.PoolManager()

        # make a snapshot
        if "NET" in os.environ:
            system = os.environ['NET']
        else:
            system = "production"

        if "RELEASE" in os.environ:
            release = os.environ['RELEASE']
        else:
            release = "0.0.0.0"

        # API_KEY Must be defined in local environment (not committed to github!)
        # if "API_KEY" in os.environ:
        #     api_access_token = os.environ.get('API_KEY')
        # else:
        #     logger.error("ERROR: please define an API_KEY in your local environment. ")
        #
        # # create a snapshot on each run of the loader
        # logger.info("making submission system snapshot")
        # snapshot_url = 'https://www.alliancegenome.org/api/data/takesnapshot?system=' \
        #                + system \
        #                + '&releaseVersion=' \
        #                + release
        # snapshot = requests.post(snapshot_url, data={"api_access_token": api_access_token})

        # use the recently created snapshot
        api_url = 'https://www.alliancegenome.org/api/data/snapshot?system=' + system + '&releaseVersion=' + release
        submission_data = http.request('GET', api_url)

        if submission_data.status != 200:
            logger.error("Status: %s" % submission_data.status)
            logger.error("No Data came from API: %s" % api_url)
            sys.exit(-1)

        self.submission_system_data = json.loads(submission_data.data.decode('UTF-8'))

        for dataFile in self.non_submission_system_data['snapShot']['dataFiles']:
            self.submission_system_data['snapShot']['dataFiles'].append(dataFile)

        # List used for MOD and data type objects.
        self.master_data_dictionary = {}

        # Dictionary for transformed submission system data.
        self.transformed_submission_system_data = {}

        # process config file during initialization
        self.process_config()
        
    def get_FT_thread_settings(self):
        return self.FileTransactorThreads

    def get_NT_thread_settings(self):
        return self.Neo4jTransactorThreads

    def get_config(self, data_type):
        # Get the object for a data type. If the object doesn't exist, this returns None.
        logger.debug("Getting config for: [%s] -> Config[%s]" % (data_type, self.master_data_dictionary))
        return self.master_data_dictionary.get(data_type)

    def dispatch_to_object(self):
        # This function sends off our data types to become DataTypeConfig objects.
        # The smaller SubTypeConfig objects are created in the DataTypeConfig functions, see data_type_config.py.
        
        for config_entry in self.transformed_submission_system_data.keys():
            # Skip string entries (e.g. schemaVersion, releaseVersion).
            if isinstance(self.transformed_submission_system_data[config_entry], str):
                continue

            logger.debug('Processing DataType: %s' % config_entry)

            # Create our data type object and add it to our master dictionary filed under the config_entry.
            # e.g. Create BGI DataTypeConfig object and file it under BGI in the dictionary.
            self.master_data_dictionary[config_entry] = DataTypeConfig(config_entry,
                                                                       self.transformed_submission_system_data[config_entry])

    def download_and_validate(self):

        logger.info('Beginning download and validation.')
        for entry in self.master_data_dictionary.keys():
            logger.debug('Downloading %s data.' % entry)
            if isinstance(self.master_data_dictionary[entry], DataTypeConfig):  # If we're dealing with an object.
                self.master_data_dictionary[entry].get_data()

    def process_config(self):
        # This checks for the validity of the YAML file.
        # See src/config/validation.yml for the layout of the schema.
        # TODO Add requirement checking and more validation to the YAML schema.

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
        
        # Query the submission system for the required data.
        self.query_submission_system()

        # Create our DataTypeConfig (which in turn create our SubTypeConfig) objects.
        self.dispatch_to_object()

    def _search_submission_data(self, dataType, subEntry):

            returned_dict = None

            # These following types are found in the local submission file
            if dataType not in ['Ontology', 'Interactions', 'GOAnnot', 'ExpressionAtlas']:
                subType = ETLHelper().get_taxon_from_MOD(subEntry)
            else:
                subType = subEntry

            try:
                returned_dict = next(item for item in self.submission_system_data['snapShot']['dataFiles']
                                     if item['dataType'] == dataType and item['taxonIDPart'] == subType)
            except StopIteration:
                logger.warn('dataType: %s subType: %s not found in submission system data.' % (dataType, subType))
                logger.warn('Creating entry with \'None\' path and extracted path.')
                returned_dict = {
                    'dataType': dataType,
                    'subType': subType,
                    'path': None,
                    'tempExtractedFile': None
                }

            return returned_dict

    def query_submission_system(self):

        # The list of tuples below is created to filter out submission system data against our config file.
        ontologies_to_transform = ('GO', 'SO', 'DO', 'MI')  # These have non-generic loaders.

        self.transformed_submission_system_data['releaseVersion'] = self.submission_system_data['snapShot']['releaseVersion']
        self.transformed_submission_system_data['schemaVersion'] = self.submission_system_data['snapShot']['schemaVersion']

        config_values_to_ignore = [
            'schemaVersion',  # Manually assigned above.
            'releaseVersion',  # Manually assigned above.
            'FileTransactorThreads',
            'Neo4jTransactorThreads'
        ]

        for entry in self.config_data.keys(): # Iterate through our config file.
            logger.debug("Entry: %s" % entry)
            if entry not in config_values_to_ignore: # Skip these entries.
                self.transformed_submission_system_data[entry] = [] # Create our empty list.
                for sub_entry in self.config_data[entry]:
                    logger.debug("Sub Entry: %s" % sub_entry)
                    submission_system_dict = self._search_submission_data(entry, sub_entry)
                    path = submission_system_dict.get('s3path')
                    tempExtractedFile = submission_system_dict.get('tempExtractedFile')
                    if tempExtractedFile is None or tempExtractedFile == '':
                        tempExtractedFile = submission_system_dict.get('s3path')

                    # Special case for storing ontologies with non-generic loaders.
                    if sub_entry in ontologies_to_transform and entry == 'Ontology':
                        self.transformed_submission_system_data[sub_entry] = []
                        self.transformed_submission_system_data[sub_entry].append([sub_entry, path, tempExtractedFile])
                    else:
                        self.transformed_submission_system_data[entry].append([sub_entry, path, tempExtractedFile])
            else:
                logger.debug("Ignoring entry: %s" % entry)
                        
        logger.debug("Loaded Types: %s" % self.transformed_submission_system_data)
