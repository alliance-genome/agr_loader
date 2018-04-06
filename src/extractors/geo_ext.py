import xml.etree.ElementTree as ElementTree
from files import XMLFile, Download


class GeoExt(object):

    def get_data(self):

        path = "tmp";
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?term=gene_geoprofiles%5Bfilter%5D+AND+%22Mus+musculus%22%5Borganism%5D&retmax=100000&db=gene"
        geo_data_file = Download(path, url, "geno-mouse").get_downloaded_file()
        print (geo_data_file)

        geo_data = XMLFile(geo_data_file).get_data()

        root = geo_data.getroot()
        for child in root:
            print(child.tag, child.attrib)
        for entrezId in root.iter('IdList'):
            print (entrezId.Id)

        return "test"

