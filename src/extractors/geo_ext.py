import xmltodict, json
from files import XMLFile, Download
from .ncbi_efetch import NCBIEfetch

class GeoExt(object):

    def get_entrez_ids(self, geoSpecies, geoTerm, geoDb, geoRetMax, geoRetrievalUrlPrefix):

        path = "tmp"
        url = NCBIEfetch(geoSpecies, geoRetMax, geoTerm, geoDb, geoRetrievalUrlPrefix).get_efetch_query_url()
        print ("efetch url: " + url)

        geo_data_file_contents = Download(path, url, "geo").get_downloaded_data()
        geo_data = json.loads(json.dumps(xmltodict.parse(geo_data_file_contents)))

        # returns result from NCBI Efetch in JSON object.
        return geo_data