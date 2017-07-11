from loaders import *
import sys
import argparse

parser = argparse.ArgumentParser(description='The root file used to launch the loader program.')

parser.add_argument('-t','--test_set', help='Defines whether a test_set is used.')
args = parser.parse_args() 

test_set = args.test_set

if __name__ == '__main__':
    al = AggregateLoader()
    al.load_from_mods(test_set = False)
    al.load_from_ontologies()
    al.load_annotations()
    al.create_indicies()