from loaders import *

if __name__ == '__main__':
    al = AggregateLoader()
    al.load_from_mods()
    al.index_data()