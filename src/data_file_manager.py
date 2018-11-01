from files import *

class DataFileManager(object):
    
    def __init__(self, configfile):
        print(configfile)

    def download_and_validate(self):
        pass

    def get_config(self, data_type):
        # More logic here to generate config object
        return DataTypeConfig(data_type)


class DataTypeConfig(object):

    def __init__(self, data_type):
        pass

    def get_data(self):
        path = "tmp";
        path_to_file = "SO/so_1.7.obo"

        S3File(path_to_file, path).download()
        return TXTFile(path + "/" + path_to_file).get_data()

    def running_etl(self):
        return True

    def has_mods():
        return ["MGI", "ZFIN"]

class ModConfig(object):

    pass
