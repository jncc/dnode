import datetime
import json
import urllib
import pycurl
import xml.etree.ElementTree as eTree

from datetime import datetime
from StringIO import StringIO
from config_manager import ConfigManager

# creates the sentinel product list.
class ProductListManager:
    POLYGON = 'POLYGON((-3.686840 53.993196, -1.392085 53.993196, -1.392085 55.819199, -3.68684 55.819199,-3.686840 53.993196))'
    SEARCH_URL_BASE = 'https://scihub.copernicus.eu/apihub/search'

    def __init__(self):
        self.config = ConfigManager("cfg.ini")

    def get_last_ingestion_date(self, productList):
        topDate = None

        for product in productList["products"]:
            date = datetime.strptime(product["ingestionDate"], '%Y-%m-%d').date()
            if topDate is None or date > topDate:
                topDate = date

        return topDate
        
    def get_search_url(self, lastIngestionDate):
        ingestionDateString = lastIngestionDate.strftime('%Y-%m-%d') + 'T00:00:00.000Z'
        criteria = {'q': 'ingestiondate:[%s TO NOW] AND footprint:"Intersects(%s)"' % (ingestionDateString, ProductListManager.POLYGON)}
        url = ProductListManager.SEARCH_URL_BASE + '?' + urllib.urlencode(criteria)

        return url

    def get_xml_data(self, url):
        
        rawDataBuffer = StringIO()

        try: 
            c = pycurl.Curl()
            c.setopt(c.URL,str(url))
            c.setopt(c.USERPWD,self.config.get_esa_credentials())
            c.setopt(c.FOLLOWLOCATION, True)
            c.setopt(c.SSL_VERIFYPEER, False)
            c.setopt(c.WRITEFUNCTION,rawDataBuffer.write)
            c.perform()
            c.close()
        except pycurl.error, e:
            msg = "Available product search failed  with error: %s" % (e.id, e.args[0]) 
            #TODO - LOG THIS, fail the search without an exception we want to continue

        return rawDataBuffer.getvalue()

    def add_products_to_list(self, rawProductsData, productList):
        try:
            root = eTree.fromstring(rawProductsData)
        except eTree.ParseError:
            raise Exception("Parse Error: %s \n %s" % (eTree.ParseError.message, rawProductsData))

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
            product = {
                "uniqueId" : uniqueId,
                "title" : title,
                "footprint" : footprint,
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


    def collect_product_ids(self, productList):
        productIds = []
        for product in productList["products"]:
            productIds.append(product["uniqueId"])
    
    def check_downloads_in_cat(self, productIds):
        dowloadedProductIds = ["31ed1d48-3e05-4156-9ec2-7bf17e98802e"]
        #TODO: Implement query against catalog api
        #catalog.get_downloadedProductIds(productIds)
        # result would be ["31ed1d48-3e05-4156-9ec2-7bf17e98802e","05dabcc8-bb50-4645-b215-43b46a6e4bf2e"]
        return dowloadedProductIds


    def create_list(self,runDate, lastListFile, outputListFile):
        productList = json.load(lastListFile)
        lastIngestionDate = self.get_last_ingestion_date(productList)

        # If latest record is older than 3 days, fail
        if (runDate - lastIngestionDate).days > 3:
            raise Exception("Last ingestion date older then 3 days")

        searchUrl = self.get_search_url(lastIngestionDate)

        rawProductsData = self.get_xml_data(searchUrl)

        self.add_products_to_list(rawProductsData, productList)

        productIds = self.collect_product_ids(productList)

        downloadedProductIds = self.check_downloads_in_cat(productIds)

        #remove downloaded products from list
        productList["products"] = [product for product in productList["products"] if product["uniqueId"] not in downloadedProductIds]

        outputListFile.write(json.dumps(productList))
