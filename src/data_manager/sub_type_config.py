from files import S3File, TXTFile, TARFile, Download
import os, logging, json
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

    def get_data_provider(self):
        return self.sub_data_type

    def get_data(self):
        # Grab the data (TODO validate).
        # Some of this algorithm is temporary.
        # e.g. Files from the submission system will arrive without the need for unzipping, etc.
        path = 'tmp'

        if self.file_to_download is not None:
            if self.file_to_download.startswith('http'):
                download_filename = os.path.basename(self.file_to_download)
                download_object = Download(path, self.file_to_download, download_filename) # savepath, urlToRetieve, filenameToSave
                self.already_downloaded = download_object.get_downloaded_data_new() # Have we already downloaded this file?
            else:
                self.already_downloaded = S3File(self.file_to_download, path).download_new()
                if self.file_to_download.endswith('tar.gz'):
                    tar_object = TARFile(path,self.file_to_download)
                    tar_object.extract_all()
        else: 
            logger.warn('No download path specified, assuming download is not required.')

    def validate(self):
        if self.already_downloaded is True:
            logger.info('File has been previously downloaded. Skipping validation.')
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

        with open(schema_file_name) as schema_file:
            schema = json.load(schema_file)

        with open(self.filepath) as data_file:
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