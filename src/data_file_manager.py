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
        path_to_file = "SO/so_1.7.obo"
        S3File(path_to_file, "tmp/").download()
        return TXTFile("tmp/" + path_to_file).get_data()

    def running_etl(self):
        return True

    def get_mod_configs(self):

        configs = []
        configs.append(ModConfig("FB_1.0.0.7_4.tar.gz", "FB_1.0.0.7_BGI.json", "Drosophila melanogaster"))
        configs.append(ModConfig("RGD_1.0.0.7_4.tar.gz", "RGD_1.0.0.7_BGI.9606.json", "Homo sapiens"))
        configs.append(ModConfig("MGI_1.0.0.7_2.tar.gz", "MGI_1.0.0.7_BGI.json", "Mus musculus"))
        configs.append(ModConfig("RGD_1.0.0.7_4.tar.gz", "RGD_1.0.0.7_BGI.10116.json", "Rattus norvegicus"))
        configs.append(ModConfig("SGD_1.0.0.7_1.tar.gz", "SGD_1.0.0.7_basicGeneInformation.json", "Saccharomyces cerevisiae"))
        configs.append(ModConfig("WB_1.0.0.7_4.tar.gz", "WB_1.0.0.7_BGI.json", "Caenorhabditis elegans"))
        configs.append(ModConfig("ZFIN_1.0.0.7_3.tar.gz", "ZFIN_1.0.0.7_basicGeneInformation.json", "Danio rerio"))
        return configs

class ModConfig(object):

    def __init__(self, tarfilename, filename, species):
        self.filename = filename
        self.tarfilename = tarfilename
        self.species = species

    def get_data(self):
        path = "tmp"
        S3File(self.tarfilename, path).download()
        TARFile(path, self.tarfilename).extract_all()
        return JSONFile().get_data(path + "/" + self.filename)

