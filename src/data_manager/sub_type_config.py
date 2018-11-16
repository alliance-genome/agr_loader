import logging
logger = logging.getLogger(__name__)

from files import S3File, TXTFile, TARFile, Download
import os, json, sys
import jsonschema as js

class SubTypeConfig(object):

    def __init__(self, data_type, sub_data_type, file_to_download, filepath):
        self.data_type = data_type
        self.sub_data_type = sub_data_type
        self.filepath = filepath
        self.file_to_download = file_to_download

        self.already_downloaded = False

    def get_filepath(self):
        return self.filepath

    def get_data_provider(self):
        return self.sub_data_type

    def get_data(self):
        # Grab the data (TODO validate).
        # Some of this algorithm is temporary.
        # e.g. Files from the submission system will arrive without the need for unzipping, etc.
        path = 'tmp'
    
        if self.filepath is not None:
            if not os.path.isfile(self.filepath):

                if self.file_to_download.startswith('http'):
                    download_filename = os.path.basename(self.file_to_download)
                    download_object = Download(path, self.file_to_download, download_filename) # savepath, urlToRetieve, filenameToSave
                    self.already_downloaded = download_object.get_downloaded_data_new() # Have we already downloaded this file?
                else:
                    self.already_downloaded = S3File(self.file_to_download, path).download_new()
                    if self.file_to_download.endswith('tar.gz'):
                        tar_object = TARFile(path,self.file_to_download)
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
            logger.info('Found temp validation file flag for %s. Skipping validation.' % self.filepath)
            return

        logger.info("Attempting to validate: %s" % (self.filepath))

        schema_lookup_dict = {
            'Disease' : 'schemas/disease/diseaseMetaDataDefinition.json',
            'BGI' : 'schemas/gene/geneMetaData.json',
            'Orthology' : 'schemas/orthology/orthologyMetaData.json',
            'Allele' : 'schemas/allele/alleleMetaData.json',
            'Phenotype' : 'schemas/phenotype/phenotypeMetaDataDefinition.json',
            'Expression' : 'schemas/expression/wildtypeExpressionMetaDataDefinition.json'
        }

        schema_file_name = schema_lookup_dict.get(self.data_type)

        if schema_file_name is None:
            logger.warn('No schema or method found. Skipping validation.')
            return # Exit validation.

        with open(schema_file_name, encoding='utf-8') as schema_file:
            schema = json.load(schema_file)

        with open(self.filepath, encoding='utf-8') as data_file:
            data = json.load(data_file)

        # Defining a resolver for relative paths and schema issues, see https://github.com/Julian/jsonschema/issues/313
        # and https://github.com/Julian/jsonschema/issues/274
        sSchemaDir = os.path.dirname(os.path.abspath(schema_file_name))
        oResolver = js.RefResolver(base_uri = 'file://' + sSchemaDir + '/', referrer = schema)

        try:
            js.validate(data, schema, format_checker=js.FormatChecker(), resolver=oResolver)
            logger.info("'%s' successfully validated against '%s'" % (self.filepath, schema_file_name))
        except js.ValidationError as e:
            logger.critical(e.message)
            logger.critical(e)
            raise SystemExit("FATAL ERROR in JSON validation.")
        except js.SchemaError as e:
            logger.critical(e.message)
            logger.critical(e)
            raise SystemExit("FATAL ERROR in JSON validation.")