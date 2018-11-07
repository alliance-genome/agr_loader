from ..files import S3File, TXTFile


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

    @staticmethod
    def has_mods():
        return ["MGI", "ZFIN"]