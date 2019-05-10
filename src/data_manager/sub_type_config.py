import logging
import os, json, sys, jsonref
from pathlib import Path
from urllib.parse import urljoin
from transactors import FileTransactor

from files import S3File, TARFile, Download
import jsonschema as js

logger = logging.getLogger(__name__)


class SubTypeConfig(object):

    def __init__(self, data_type, sub_data_type, file_to_download, filepath):
        self.data_type = data_type
        self.sub_data_type = sub_data_type
        self.filepath = filepath
        self.file_to_download = file_to_download

        self.already_downloaded = False

    def get_filepath(self):
        return self.filepath
    
    def get_file_to_download(self):
        return self.file_to_download

    def get_data_provider(self):
        return self.sub_data_type

    def get_data(self):
        # Grab the data (TODO validate).
        # Some of this algorithm is temporary.
        # e.g. Files from the submission system will arrive without the need for unzipping, etc.
        path = 'tmp'
    
        if self.filepath is not None:
            if not os.path.isfile(self.filepath):
                logger.debug("File to download: " + self.file_to_download)
                if self.file_to_download.startswith('http'):
                    download_filename = os.path.basename(self.filepath)
                    logger.debug("Download Name: " + download_filename)
                    download_object = Download(path, self.file_to_download, download_filename)
                    self.already_downloaded = download_object.get_downloaded_data_new()
                else:
                    logger.debug("Downloading JSON File: " + self.file_to_download)
                    self.already_downloaded = S3File(self.file_to_download, path).download_new()
                    logger.debug("File already downloaded: %s" % (self.already_downloaded))
                    if self.file_to_download.endswith('tar.gz'):
                        logger.debug("Extracting all files: %s" % (self.file_to_download))
                        tar_object = TARFile(path, self.file_to_download)
                        tar_object.extract_all()
                        # Check whether the file exists locally.
                if self.filepath is not None:
                    try:
                        os.path.isfile(self.filepath)
                    except:
                        logger.critical('No local copy of the specified file found!')
                        logger.critical('Missing copy of %s for sub type: %s from data type: %s' % (self.filepath, self.sub_data_type, self.data_type))
                        logger.critical('Please check download functions or data source.')
                        sys.exit(-1)
            else:
                logger.debug("File Path already downloaded: %s" % (self.filepath))
        else:
            logger.debug("File Path is None not downloading")

    def validate(self):
        if self.filepath is None:
            logger.warn('No filepath found for sub type: %s from data type: %s ' % (self.sub_data_type, self.data_type))
            logger.warn('Skipping validation.')
            return

        # TODO -- The method below can be reworked once we switch to the submission system.

        # Begin temporary validation skipping method.
        # This method attempts to create a _temp_val_check file via "open"
        # If the file exists, it means the validation has already run and we should skip it.
        # If the file doesn't exist, we should create it and run the validation.
        self.already_downloaded = False
        
        try:
            open(self.filepath + '_temp_val_check', 'x')
        except FileExistsError:
            self.already_downloaded = True
        except TypeError: # if self.filepath is "None".
            pass
        # End of temporary validation method.

        # The code below can run "as is" for validation skipping using the Download / S3 methods to check for existing files.
        # The submission system needs to be in place (files are downloaded as .json) for this to work.
        if self.already_downloaded is True:
            logger.debug('Found temp validation file flag for %s. Skipping validation.' % self.filepath)
            return

        logger.debug("Attempting to validate: %s" % (self.filepath))

        schema_lookup_dict = {
            'Disease': 'schemas/disease/diseaseMetaDataDefinition.json',
            'BGI': 'schemas/gene/geneMetaData.json',
            'Orthology': 'schemas/orthology/orthologyMetaData.json',
            'Allele': 'schemas/allele/alleleMetaData.json',
            'Phenotype': 'schemas/phenotype/phenotypeMetaDataDefinition.json',
            'Expression': 'schemas/expression/wildtypeExpressionMetaDataDefinition.json'
        }

        schema_file_name = schema_lookup_dict.get(self.data_type)

        if schema_file_name is None:
            logger.warn('No schema or method found. Skipping validation.')
            return  # Exit validation.

        with open(self.filepath, encoding='utf-8') as data_file:
            data = json.load(data_file)

        # These variables are used to dynamically "fill out" all the references in the schema file.
        base_dir_url = Path(os.path.realpath(os.getcwd())).as_uri() + '/'
        base_file_url = urljoin(base_dir_url, schema_file_name)

        with open(schema_file_name, encoding='utf-8') as schema_file:
            # jsonref builds out our json #ref for the schema validation to work correctly.
            expanded_schema_file = jsonref.load(schema_file, base_uri=base_file_url)

        try:
            js.validate(data, expanded_schema_file)
            logger.debug("'%s' successfully validated against '%s'" % (self.filepath, schema_file_name))
        except js.ValidationError as e:
            logger.critical(e.message)
            logger.critical(e)
            raise SystemExit("FATAL ERROR in JSON validation.")
        except js.SchemaError as e:
            logger.critical(e.message)
            logger.critical(e)
            raise SystemExit("FATAL ERROR in JSON validation.")
