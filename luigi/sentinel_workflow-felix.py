import luigi
from luigi.s3 import S3Target

class YesterdaysAvailableListTask(luigi.ExternalTask):    
    yesterday = luigi.Parameter()
    def output(self):
	    return S3Target('s3://jncc-data/sentinel-' + yesterday + 'example/hello.txt')
    

class SentinelListTask(luigi.Task)


# SentinelListTask
# ================
# SentinelListTask/2016-11-13/available.json  (files to download)
# query esa hub for all products ingested in last 2 weeks (in available.json)
# query catalog for all products in that set that have been downloaded and subtract


let window = now - 2 weeks 
let esa = esaListService

products = esa.GetProductMetadata(ingestedFrom:window.Start, ingestedTo:window.End)

let catalog = eodipCatalogService

for each product in products
    if catalog.contains(product.id)
        products.remove(product)

write products to "SentinelListTask/" + date.today + "/available.json" on s3 pot

# available.json
# [{"id":"dd","title":"S1_DDDD"},{"id":"dd","title":"S1_DDDE"}]

# notes:
# Handles files taht won't download, they will fall outside of the moving window
# Potentially means getting a large set of data from esa 
# Won't continue failed downloads if the esa metadata service is down. The list depends on esa being up.


# 
# Luigi - SentinelDownloadTask 
# ====================
let availableProducts = read("SentinelListTask/" + date.today + "/available.json")

let downloadTask = sentinelDownloader

for each product in products
    new downloadTask.download(product)


# downloadTask(product)
# ====================
downloadTask(product) 
    let esa = esaDownloadService

    productZipFile = esa.GetProduct(product.id)

    if productZipFile != null
        write productZipFile to "SentinelRepo/" + product.title
        product.AddProperty("location") = "SentinelRepo/" + product.title
        write product to "SentinelDownloadTask/" + date.today + "/" + product.title + ".json"
    

# "SentinelDownloadTask/" + date.today + "/" + product.title + ".json"
{"id":"dd","title":"S1_DDDD","location":"s3://sentnel/raw/S1_DDDD.zip"}

# SentinelCatalogTask
# ====================
let file = s3FileSystem.GetFileList("SentinelDownloadTask/" + date.today)

let catalog = eodipCatalogService

for each file in files
    product = s3FileSystem.read(file)
    catalog.addProduct(product)


'


