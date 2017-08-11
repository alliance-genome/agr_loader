from loaders import *
from aggregate_loader import AggregateLoader
import os

useTestObject = os.environ['TEST_SET']
if useTestObject == "True":
	useTestObject = True # Convert string to boolean. TODO a better method?

if __name__ == '__main__':
    al = AggregateLoader()
    al.create_indicies()
    al.load_from_mods(useTestObject)
    al.load_annotations()
    al.load_from_ontologies()


