from files import JSONFile, TXTFile, S3File, TARFile

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

    def get_neo4j_commit_size(self):
        return 2500

    def get_generator_batch_size(self):
        return 10000

    def get_mod_configs(self):

        configs = []
        
        configs.append(ModConfig(
            "FB_1.0.0.7_4.tar.gz",
            "FB_1.0.0.7_BGI.json",
            "FB_1.0.0.7_feature.json",
            "FB_1.0.0.7_disease.json",
            "FB_1.0.0.7_phenotype.json",
            "FB_1.0.0.7_expression.json",
            "FB"))

        configs.append(ModConfig(
            "RGD_1.0.0.7_4.tar.gz",
            "RGD_1.0.0.7_BGI.9606.json",
            "", # None for Human.
            "RGD_1.0.0.7_disease.9606.json",
            "RGD_1.0.0.7_phenotype.9606.json",
            "RGD_1.0.0.7_expression.9606.json",
            "RGD"))

        configs.append(ModConfig(
            "MGI_1.0.0.7_2.tar.gz",
            "MGI_1.0.0.7_BGI.json",
            "MGI_1.0.0.7_allele.json",
            "MGI_1.0.0.7_disease.json",
            "MGI_1.0.0.7_phenotype.json",
            "MGI_1.0.0.7_expression.json",
            "MGI"))

        configs.append(ModConfig(
            "RGD_1.0.0.7_4.tar.gz",
            "RGD_1.0.0.7_BGI.10116.json",
            "RGD_1.0.0.7_allele.10116.json",
            "RGD_1.0.0.7_disease.10116.json",
            "RGD_1.0.0.7_phenotype.10116.json",
            "RGD_1.0.0.7_expression.10116.json",
            "RGD"))

        configs.append(ModConfig(
            "SGD_1.0.0.7_1.tar.gz",
            "SGD_1.0.0.7_basicGeneInformation.json",
            "", # None for SGD.
            "SGD_1.0.0.7_disease.daf.json",
            "SGD_1.0.0.7_phenotype.json",
            "SGD_1.0.0.7_expression.json",
            "SGD"))

        configs.append(ModConfig(
            "WB_1.0.0.7_4.tar.gz",
            "WB_1.0.0.7_BGI.json",
            "WB_1.0.0.7_allele.json",
            "WB_1.0.0.7_disease.json",
            "WB_1.0.0.7_phenotype.json",
            "WB_1.0.0.7_expression.json",
            "WB"))

        configs.append(ModConfig(
            "ZFIN_1.0.0.7_3.tar.gz",
            "ZFIN_1.0.0.7_basicGeneInformation.json",
            "ZFIN_1.0.0.7_allele.json",
            "ZFIN_1.0.0.7_disease.daf.json",
            "ZFIN_1.0.0.7_phenotype.json",
            "ZFIN_1.0.0.7_expression.json",
            "ZFIN"))

        return configs

class ModConfig(object):

    def __init__(self, tarfilename, bgifilename, allelefilename, diseaseFileName, phenotypeFileName, expressionFileName, data_provider):
        self.tarfilename = tarfilename
        self.bgifilename = bgifilename
        self.allelefilename = allelefilename
        self.diseaseFileName = diseaseFileName
        self.phenotypeFileName = phenotypeFileName
        self.expressionFileName = expressionFileName
        self.data_provider = data_provider
        self.path = "tmp"

    def get_bgi_data(self):
        return self._get_json_data(self.bgifilename)

    def get_allele_data(self):
        return self._get_json_data(self.allelefilename)

    def get_disease_data(self):
        return self._get_json_data(self.diseaseFileName)

    def get_expression_file_name(self):
        S3File(self.tarfilename, self.path).download()
        TARFile(self.path, self.tarfilename).extract_all()
        return self.path + "/" + self.expressionFileName

    def _get_json_data(self, filename):
        if len(filename) > 0:
            S3File(self.tarfilename, self.path).download()
            TARFile(self.path, self.tarfilename).extract_all()
            return JSONFile().get_data(self.path + "/" + filename)
        else:
            return None
        