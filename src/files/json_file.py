import json
import codecs
import jsonschema as js
import os

class JSONFile(object):

    def get_data(self, filename, jsonType):
        print("Loading json data from %s ..." % (filename))
        with codecs.open(filename, 'r', 'utf-8') as f:
            data = json.load(f)
        f.close()
        self.validate_json(data, filename, jsonType)
        return data

    def validate_json(self, data, filename, jsonType):
        print("Validating %s JSON." % (jsonType))


        schema_file_name = None
        if jsonType == 'disease':
            schema_file_name = 'schemas/disease/diseaseMetaDataDefinition.json'
        elif jsonType == 'BGI':
            schema_file_name = 'schemas/gene/geneMetaData.json'
        elif jsonType == 'orthology':
            schema_file_name = 'schemas/orthology/orthoMetaData.json'
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
            print("'%s' successfully validated against '%s'" % (filename, schema_file_name))
        except js.ValidationError as e:
            print(e.message)
            print(e)
            raise SystemExit("FATAL ERROR in JSON validation.")
        except js.SchemaError as e:
            print(e.message)
            print(e)
            raise SystemExit("FATAL ERROR in JSON validation.")
