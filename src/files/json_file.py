import json
import codecs
import jsonschema as js
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class JSONFile(object):

    def get_data(self, filename):
        logger.info("Loading json data from %s ..." % filename)
        with codecs.open(filename, 'r', 'utf-8') as f:
            logger.info ("file open")
            data = json.load(f)
            logger.info ("json data extracted")
        f.close()
        #self.validate_json(data, filename, jsonType)
        return data

    def validate_json(self, data, filename, jsonType):
        logger.info("Validating %s JSON." % (jsonType))

        schema_file_name = None
        if jsonType == 'disease':
            schema_file_name = 'schemas/disease/diseaseMetaDataDefinition.json'
        elif jsonType == 'BGI':
            schema_file_name = 'schemas/gene/geneMetaData.json'
        elif jsonType == 'orthology':
            schema_file_name = 'schemas/orthology/orthologyMetaData.json'
        elif jsonType == 'allele':
            schema_file_name = 'schemas/allele/alleleMetaData.json'
        elif jsonType == 'phenotype':
            schema_file_name = 'schemas/phenotype/phenotypeMetaDataDefinition.json'
        elif jsonType == 'expression':
            schema_file_name = 'schemas/expression/wildtypeExpressionMetaDataDefinition.json'

        with open(schema_file_name) as schema_file:
            schema = json.load(schema_file)

        # Defining a resolver for relative paths and schema issues, see https://github.com/Julian/jsonschema/issues/313
        # and https://github.com/Julian/jsonschema/issues/274
        sSchemaDir = os.path.dirname(os.path.abspath(schema_file_name))
        oResolver = js.RefResolver(base_uri = 'file://' + sSchemaDir + '/', referrer = schema)

        try:
            js.validate(data, schema, format_checker=js.FormatChecker(), resolver=oResolver)
            logger.info("'%s' successfully validated against '%s'" % (filename, schema_file_name))
        except js.ValidationError as e:
            logger.info(e.message)
            logger.info(e)
            raise SystemExit("FATAL ERROR in JSON validation.")
        except js.SchemaError as e:
            logger.info(e.message)
            logger.info(e)
            raise SystemExit("FATAL ERROR in JSON validation.")
