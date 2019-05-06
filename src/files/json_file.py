import logging
logger = logging.getLogger(__name__)

import codecs
import json
import os

import jsonschema as js


class JSONFile(object):

    def get_data(self, filename):
        logger.debug("Loading JSON data from %s ..." % filename)
        if 'PHENOTYPE' in filename:
            self.remove_bom_inplace(filename)
        with codecs.open(filename, 'r', 'utf-8') as f:
            logger.debug("Opening JSONFile: %s" % filename)
            data = json.load(f)
            logger.debug("JSON data extracted %s" % filename)
        f.close()
        #self.validate_json(data, filename, jsonType)
        return data

    def validate_json(self, data, filename, jsonType):
        logger.debug("Validating %s JSON." % (jsonType))

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
            logger.debug("'%s' successfully validated against '%s'" % (filename, schema_file_name))
        except js.ValidationError as e:
            logger.info(e.message)
            logger.info(e)
            raise SystemExit("FATAL ERROR in JSON validation.")
        except js.SchemaError as e:
            logger.info(e.message)
            logger.info(e)
            raise SystemExit("FATAL ERROR in JSON validation.")

    def remove_bom_inplace(self, path):
        """Removes BOM mark, if it exists, from a file and rewrites it in-place"""
        buffer_size = 4096
        bom_length = len(codecs.BOM_UTF8)

        with codecs.open(path, "r+b") as fp:
            chunk = fp.read(buffer_size)
            if chunk.startswith(codecs.BOM_UTF8):
                i = 0
                chunk = chunk[bom_length:]
                while chunk:
                    fp.seek(i)
                    fp.write(chunk)
                    i += len(chunk)
                    fp.seek(bom_length, os.SEEK_CUR)
                    chunk = fp.read(buffer_size)
                fp.seek(-bom_length, os.SEEK_CUR)
                fp.truncate()
