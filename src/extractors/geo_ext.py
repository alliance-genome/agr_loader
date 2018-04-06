from files import Download, TXTFile, XMLFile

class GeoExt():

    def get_data(self):

        path = "tmp";
        XMLFile(path, url, "geo-mouse").download()
        geo_data = XMLFile(path + "/geo-mouse").get_data()
