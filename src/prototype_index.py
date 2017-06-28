from loaders import *

if __name__ == '__main__':
    al = PrototypeAggregateLoader()
    al.load_from_mods()
    al.index_data()