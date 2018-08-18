from aggregate_loader import AggregateLoader
import os
import time

useTestObject = os.environ['TEST_SET']
if useTestObject == "True":
    useTestObject = True # Convert string to boolean. TODO a better method?

host = os.environ['NEO4J_NQC_HOST']
port = os.environ['NEO4J_NQC_PORT']
uri = "bolt://" + host + ":" + port

if __name__ == '__main__':
    al = AggregateLoader(uri, useTestObject)
    al.load_from_ont("http://data.bioontology.org/ontologies/MA/submissions/126/download?apikey=8b5b7825-538d-40e0-9e9e-5ab9274a9aeb", "MA", "ma.obo")
