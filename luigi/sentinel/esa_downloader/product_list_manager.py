import datetime
import json
import urllib
import pycurl
import logging
import math
import geojson
import shapely
import xml.etree.ElementTree as eTree
import log_helper
import shapely.wkt



from functional import seq
from datetime import datetime
from dateutil import parser
from StringIO import StringIO
from config_manager import ConfigManager
from catalog_manager import CatalogManager
from shapely.geometry import shape

# creates the sentinel product list.


class ProductListManager:
    POLYGON = 'POLYGON ((-6.604981356192942 49.438680703689379,-10.186858447403869 60.557572594302513,0.518974191882126 61.368840444480654,2.668100446608686 53.215944284612512,1.235349610124312 50.589462482174554,-6.604981356192942 49.438680703689379))'
    SEARCH_URL_BASE = 'https://scihub.copernicus.eu/apihub/search'

    def __init__(self, debug):
        self.config = ConfigManager("cfg.ini")
        self.debug = debug
        self.log = log_helper.setup_logging('CreateAvailableProductsList', self.debug)

    def __get_last_ingestion_date(self, productList):
        topDate = None

        for product in productList["products"]:
            date = parser.parse(product["ingestionDate"]).date()
            if topDate is None or date > topDate:
                topDate = date

        return topDate

    def __get_search_url(self, lastIngestionDate, page):
        ingestionDateString = lastIngestionDate.strftime(
            '%Y-%m-%d') + 'T00:00:00.000Z'
        criteria = {'q': 'ingestiondate:[%s TO NOW] AND footprint:"Intersects(%s)"' % (
            ingestionDateString, ProductListManager.POLYGON)}
        url = ProductListManager.SEARCH_URL_BASE + \
            '?' + urllib.urlencode(criteria)

        return url

    def __get_xml_data(self, url):

        rawDataBuffer = StringIO()

        try:
            c = pycurl.Curl()
            c.setopt(c.URL, str(url))
            c.setopt(c.USERPWD, self.config.get_esa_credentials())
            c.setopt(c.FOLLOWLOCATION, True)
            c.setopt(c.SSL_VERIFYPEER, False)
            c.setopt(c.WRITEFUNCTION, rawDataBuffer.write)
            c.perform()
            c.close()
        except pycurl.error, e:
            msg = "Available product search failed  with error: %s" % (e.args[0],)
            self.log.error(msg)
            # fail the search without an exception we want to continue

        return rawDataBuffer.getvalue()

    def __get_xml_element_tree(self, data):
        root = None
        try:
            root = eTree.fromstring(data)
        except eTree.ParseError:
            raise Exception("Parse Error: %s \n %s" %
                            (eTree.ParseError.message, data))

        return root

    def __getGeometry(self, footprintText):
        
        footprint = {}
        centroid = {}
        geom = None

        try:
            feature = geojson.loads(footprintText)
            footprint = feature.geometry
            geom = shape(feature)
        except ValueError, e:
            # probably failed because footprintText is wkt
            geom = shapely.wkt.loads(footprintText)
            feature = geojson.Feature(geometry=geom)
            footprint = feature.geometry

        if not "crs" in footprint:
            footprint["crs"] = {"type":"name","properties":{"name":"EPSG:4326"}}

        centroidGeom = geom.centroid
        centroidFeature = geojson.Feature(geometry=centroidGeom)
        centroid = centroidFeature.geometry
        centroid["crs"] = {"type":"name","properties":{"name":"EPSG:4326"}}

        return {
            "footprint": footprint,
            "centroid" : centroid
        }
    
        
    def __add_products_to_list(self, rawProductsData, productList):
        root = self.__get_xml_element_tree(rawProductsData)

        for entry in root.iter('{http://www.w3.org/2005/Atom}entry'):
            uniqueId = entry.find('{http://www.w3.org/2005/Atom}id').text
            title = entry.find('{http://www.w3.org/2005/Atom}title').text
            footprint = ''
            orbitDirection = ''
            productType = ''
            beginPosition = ''
            endPosition = ''
            ingestionDate = ''
            for string in entry.iter('{http://www.w3.org/2005/Atom}str'):
                if string.attrib.has_key('name'):
                    if string.attrib['name'] == 'footprint':
                        footprint = string.text
                    if string.attrib['name'] == 'orbitdirection':
                        orbitDirection = string.text
                    if string.attrib['name'] == 'producttype':
                        productType = string.text
                    if string.attrib['name'] == 'platformname':
                        platform = string.text
            for string in entry.iter('{http://www.w3.org/2005/Atom}date'):
                if string.attrib.has_key('name'):
                    if string.attrib['name'] == 'ingestiondate':
                        ingestionDate = string.text
                    if string.attrib['name'] == 'beginposition':
                        beginPosition = string.text
                    if string.attrib['name'] == 'endposition':
                        endPosition = string.text
            for string in entry.iter('{http://www.w3.org/2005/Atom}int'):
                if string.attrib.has_key('name'):
                    if string.attrib['name'] == 'orbitnumber':
                        orbitNo = string.text
                    if string.attrib['name'] == 'relativeorbitnumber':
                        relOrbitNo = string.text

            geom = self.__getGeometry(footprint)

            product = {
                "uniqueId" : uniqueId,
                "title" : title,
                "footprint" : geom["footprint"],
                "centroid" : geom["centroid"],
                "productType" : productType,
                "beginPosition" : beginPosition,
                "endPosition" : endPosition,
                "ingestionDate" : ingestionDate,
                "platform" : platform,
                "orbitDirection" : orbitDirection,
                "orbitNo" : orbitNo,
                "relOrbitNo" : relOrbitNo
                }
        
            productList["products"].append(product)

    def __getPages(self, rawProductsData):
        root = self.__get_xml_element_tree(rawProductsData)

        pages = 1

        totalResults = int(root.find('{http://a9.com/-/spec/opensearch/1.1/}totalResults').text)

        if totalResults > 100:
            pages = int(math.ceil(totalResults / 100))
        
        return pages

    def create_list(self,runDate, productList, outputListFile, seedDate):
        lastIngestionDate = None

        if not seeding:
            lastIngestionDate = self.__get_last_ingestion_date(productList)
            # If latest record is older than 3 days, fail
            if lastIngestionDate is None:
                raise Exception("Unable to determine last ingestion date")
            if (runDate - lastIngestionDate).days > 3:
                raise Exception("Last ingestion date older then 3 days")
        else:
            lastIngestionDate = seedDate

        if lastIngestionDate is None:
            raise Exception("Unable to determine last ingestion date")

        page = 1
        pages = 1

        searchUrl = self.__get_search_url(lastIngestionDate, page)
        rawProductsData = self.__get_xml_data(searchUrl)

        pages = self.__getPages(rawProductsData)

        while page <= pages:
            self.__add_products_to_list(rawProductsData, productList)
            page = page + 1

            searchUrl = self.__get_search_url(lastIngestionDate, page)
            rawProductsData = self.__get_xml_data(searchUrl)
            if rawProductsData == None:
                break

        # remove duplicate products
        # remove products that are already in the catalog
        with CatalogManager() as cat:
            productList["products"] = (seq(productList["products"])
                                        .distinct_by(lambda x: x["uniqueId"])
                                        .filter(lambda x: cat.exists(x["uniqueId"]) != True )).to_list()

        outputListFile.write(json.dumps(productList))
