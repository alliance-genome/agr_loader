from aggregate_loader import AggregateLoader
import os

useTestObject = os.environ['TEST_SET']
if useTestObject == "True":
    useTestObject = True # Convert string to boolean. TODO a better method?

host = os.environ['NEO4J_NQC_HOST']
port = os.environ['NEO4J_NQC_PORT']
uri = "bolt://" + host + ":" + port

if __name__ == '__main__':
    al = AggregateLoader(uri, useTestObject)

    # The following order is REQUIRED for proper loading.
    al.create_indicies()
    al.load_resource_descriptors()
    al.load_from_ontologies()
    al.load_from_mods()
