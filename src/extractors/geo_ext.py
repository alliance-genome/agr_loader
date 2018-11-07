import xmltodict, json, urllib
from files import Download
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class GeoExt(object):

    def get_entrez_ids(self, species, geoTerm, geoDb, geoRetMax, geoRetrievalUrlPrefix):

        path = "tmp"

        url = geoRetrievalUrlPrefix + "term=" + geoTerm + "[filter]" + "+AND+" + urllib.parse.quote_plus(species) + "[Organism]" + "&retmax=" + geoRetMax + "&db=" + geoDb


        logger.info ("efetch url: " + url)

        geo_data_file_contents = Download(path, url, "geo").get_downloaded_data()
        geo_data = json.loads(json.dumps(xmltodict.parse(geo_data_file_contents)))

        # returns result from NCBI Efetch in JSON object.
        return geo_data
