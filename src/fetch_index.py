from loaders import *
from aggregate_loader import AggregateLoader
import os

test_set = os.environ['TEST_SET']
if test_set == "True":
	test_set = True # Convert string to boolean. TODO a better method?

if __name__ == '__main__':
    al = AggregateLoader()
    al.create_indicies()
    #al.load_from_mods(test_set=test_set)
    al.load_from_ontologies()
    #al.load_annotations()

