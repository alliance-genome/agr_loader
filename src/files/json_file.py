'''JSON File'''

import logging
import codecs
import json
import os
import jsonschema as js


class JSONFile():
    '''JSON File'''

    logger = logging.getLogger(__name__)

    def get_data(self, filename):
        '''Get Data'''

        self.logger.debug("Loading JSON data from %s ...", filename)

        if 'PHENOTYPE' in filename:
            self.logger.info(filename)
            self.remove_bom_inplace(filename)
        with codecs.open(filename, 'r', 'utf-8') as file_handle:
            self.logger.debug("Opening JSON file: %s", filename)
            data = json.load(file_handle)
            self.logger.debug("JSON data extracted %s", filename)

        #self.validate_json(data, filename, jsonType)
        return data

    def validate_json(self, data, filename, json_type):
        '''Validate JSON'''

        self.logger.debug("Validating %s JSON.", json_type)

        schema_file_name = None
        if json_type == 'disease':
            schema_file_name = 'schemas/disease/diseaseMetaDataDefinition.json'
        elif json_type == 'BGI':
            schema_file_name = 'schemas/gene/geneMetaData.json'
        elif json_type == 'orthology':
            schema_file_name = 'schemas/orthology/orthologyMetaData.json'
        elif json_type == 'allele':
            schema_file_name = 'schemas/allele/alleleMetaData.json'
        elif json_type == 'phenotype':
            schema_file_name = 'schemas/phenotype/phenotypeMetaDataDefinition.json'
        elif json_type == 'expression':
            schema_file_name = 'schemas/expression/wildtypeExpressionMetaDataDefinition.json'
        elif json_type == 'constructs':
            schema_file_name = 'schemas/construct/constructMetaDataDefinition.json'

        with open(schema_file_name) as schema_file:
            schema = json.load(schema_file)

        # Defining a resolver for relative paths and schema issues,
        # see https://github.com/Julian/jsonschema/issues/313
        #     and https://github.com/Julian/jsonschema/issues/274
        s_schema_dir = os.path.dirname(os.path.abspath(schema_file_name))
        o_resolver = js.RefResolver(base_uri='file://' + s_schema_dir + '/', referrer=schema)

        try:
            js.validate(data, schema, format_checker=js.FormatChecker(), resolver=o_resolver)
            self.logger.debug("'%s' successfully validated against '%s'",
                              filename, schema_file_name)
        except js.ValidationError as error:
            self.logger.info(error.message)
            self.logger.info(error)
            raise SystemExit("FATAL ERROR in JSON validation.")
        except js.SchemaError as error:
            self.logger.info(error.message)
            self.logger.info(error)
            raise SystemExit("FATAL ERROR in JSON validation.")

    @staticmethod
    def remove_bom_inplace(path):
        '''Removes BOM mark, if it exists, from a file and rewrites it in-place'''

        buffer_size = 4096
        bom_length = len(codecs.BOM_UTF8)

        with codecs.open(path, "r+b") as file_handle:
            chunk = file_handle.read(buffer_size)
            if chunk.startswith(codecs.BOM_UTF8):
                i = 0
                chunk = chunk[bom_length:]
                while chunk:
                    file_handle.seek(i)
                    file_handle.write(chunk)
                    i += len(chunk)
                    file_handle.seek(bom_length, os.SEEK_CUR)
                    chunk = file_handle.read(buffer_size)
                file_handle.seek(-bom_length, os.SEEK_CUR)
                file_handle.truncate()
