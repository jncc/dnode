import datetime
import json
import urllib
import pycurl
import logging
import math
import geojson
import shapely
import xml.etree.ElementTree as eTree
import logging
import shapely.wkt
import constants

from functional import seq
from datetime import datetime
from dateutil import parser
from io import StringIO
from config_manager import ConfigManager
from catalog_manager import CatalogManager
from shapely.geometry import shape

# creates the sentinel product list.


class ProductListManager:
   # POLYGON = 'POLYGON ((-6.604981356192942 49.438680703689379,-10.186858447403869 60.557572594302513,0.518974191882126 61.368840444480654,2.668100446608686 53.215944284612512,1.235349610124312 50.589462482174554,-6.604981356192942 49.438680703689379))'
    SEARCH_URL_BASE = 'https://scihub.copernicus.eu/apihub/search'

    def __init__(self, debug):
        self.config = ConfigManager("app.cfg")
        self.debug = debug
        self.logger = logging.getLogger('luigi-interface') 


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

        q = 'ingestiondate:[%s TO NOW] AND footprint:"Intersects(%s)"' % (
            ingestionDateString, self.config.get_search_polygon())
        
        if self.config.get_esa_searchCriteria() != None:
            q  = '%s AND %s' % (q, self.config.get_esa_searchCriteria())

        criteria = {
            'start' : page,
            'rows' : 100,
            'q': q
            }

        url = ProductListManager.SEARCH_URL_BASE + \
            '?' + urllib.urlencode(criteria)

        if self.debug:
            self.logger.info("search url %s", url)

        return url

    def __get_xml_data(self, url, esaCredentials):

        rawDataBuffer = StringIO()

        try:
            c = pycurl.Curl()
            c.setopt(c.URL, str(url))
            c.setopt(c.USERPWD, esaCredentials)
            c.setopt(c.FOLLOWLOCATION, True)
            c.setopt(c.SSL_VERIFYPEER, False)
            c.setopt(c.WRITEFUNCTION, rawDataBuffer.write)
            c.perform()
            c.close()
        except pycurl.error as e:
            msg = "Available product search failed  with error: %s" % (e.args[0],)
            self.loggerger.error(msg)
            # fail the search without an exception we want to continue

        return rawDataBuffer.getvalue()

    def __get_xml_element_tree(self, data):
        root = None
        try:
            root = eTree.fromstring(data)
        except eTree.ParseError as e:
            raise Exception("Parse Error: %s \n %s" %
                            (e.message, data))

        return root

    def __getGeometry(self, footprintText):
        
        footprint = {}
        centroid = {}
        geom = None

        try:
            feature = geojson.loads(footprintText)
            footprint = feature.geometry
            geom = shape(feature)
        except ValueError as e:
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

    def __get_pages(self, rawProductsData):
        root = self.__get_xml_element_tree(rawProductsData)

        pages = 1

        totalResults = int(root.find('{http://a9.com/-/spec/opensearch/1.1/}totalResults').text)

        if totalResults > 100:
            pages = int(math.ceil(totalResults / 100))
        
        return pages

    def create_list(self,runDate, productList, outputListFile, seedDate, esaCredentials, dbConnectionString):
        lastIngestionDate = None

        if seedDate == constants.DEFAULT_DATE:
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

        page = 0
        pages = 1

        searchUrl = self.__get_search_url(lastIngestionDate, page)
        rawProductsData = self.__get_xml_data(searchUrl, esaCredentials)

        pages = self.__get_pages(rawProductsData)

        while page <= (pages - 1):
            self.__add_products_to_list(rawProductsData, productList)
            page = page + 1

            searchUrl = self.__get_search_url(lastIngestionDate, page)
            rawProductsData = self.__get_xml_data(searchUrl, esaCredentials)
            if rawProductsData == None:
                break

        # remove duplicate products
        # remove products that are already in the catalog
        with CatalogManager(dbConnectionString) as cat:
            productList["products"] = (seq(productList["products"])
                                        .distinct_by(lambda x: x["uniqueId"])
                                        .filter(lambda x: cat.exists(x["uniqueId"]) != True )).to_list()

        outputListFile.write(json.dumps(productList))
