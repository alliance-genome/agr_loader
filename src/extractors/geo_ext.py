import xmltodict, json
from files import XMLFile, Download
from extractors import NCBIEfetch

class GeoExt(object):

    def get_data(self):

        path = "tmp";

        url = NCBIEfetch("Mus+musculus", "100000", "gene_geoprofiles", "gene", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?").get_efetch_query_url()
        print ("efetch url: " + url)

        geo_data_file_contents = Download(path, url, "geo-mouse").get_downloaded_file()
        entrezIds = []
        data = json.loads(json.dumps(xmltodict.parse(geo_data_file_contents)))
        for k, v in data.items():
            print (k, v)
            if k == 'IdList':
                for entrezId in k:
                   entrezIds.append(entrezId)

        return entrezIds


