from aggregate_loader import AggregateLoader
import os
import gc

useTestObject = os.environ['TEST_SET']
if useTestObject == "True":
    useTestObject = True # Convert string to boolean. TODO a better method?

host = os.environ['NEO4J_NQC_HOST']
port = os.environ['NEO4J_NQC_PORT']
uri = "bolt://" + host + ":" + port

if __name__ == '__main__':
    al = AggregateLoader(uri, useTestObject)
    # The following order is REQUIRED for proper loading.
    al.create_indices()
    al.load_from_ontologies()
    al.load_from_mods()
    gc.collect()
    al.load_additional_datasets()
    # TODO get working on build server
    #al.add_inferred_disease_annotations()
