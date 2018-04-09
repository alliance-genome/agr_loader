import xmltodict, json
from files import XMLFile, Download


class GeoExt(object):

    def get_data(self):

        path = "tmp";
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?term=gene_geoprofiles%5Bfilter%5D+AND+%22Mus+musculus%22%5Borganism%5D&retmax=100000&db=gene"

        geo_data_file_contents = Download(path, url, "geno-mouse").get_downloaded_file()
        entrezIds = []
        data = json.loads(json.dumps(xmltodict.parse(geo_data_file_contents)))
        for k, v in data.items():
            print (k, v)
            if k == 'IdList':
                for entrezId in k:
                   entrezIds.append(entrezId)

        return entrezIds

