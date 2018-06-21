class DataProvider(object):

    def get_data_provider(self, species):
        if species == 'Danio rerio':
            return 'ZFIN'
        elif species == 'Mus musculus':
            return 'MGI'
        elif species == 'Drosophila melanogaster':
            return 'FB'
        elif species == 'Homo sapiens':
            return 'RGD'
        elif species == 'Rattus norvegicus':
            return 'RGD'
        elif species == 'Caenorhabditis elegans':
            return 'WB'
        elif species == 'Saccharomyces cerevisiae':
            return 'SGD'
        else:
            return 'Alliance'
