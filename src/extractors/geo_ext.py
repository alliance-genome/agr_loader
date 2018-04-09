import xmltodict, json
from files import XMLFile, Download
from .ncbi_efetch import NCBIEfetch

class GeoExt(object):

    def get_entrez_ids(self, geoSpecies):

        path = "tmp"
        url = NCBIEfetch(geoSpecies, "10", "gene_geoprofiles", "gene", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?").get_efetch_query_url()
        print ("efetch url: " + url)

        geo_data_file_contents = Download(path, url, "geo-mouse").get_downloaded_file()
        entrezIds = []
        data = json.loads(json.dumps(xmltodict.parse(geo_data_file_contents)))
        for efetchKeys, efetchValues in data.items():
            if efetchKeys == 'IdList':
                for entrezId in efetchKeys:
                   entrezIds.append(entrezId)

        return entrezIds


