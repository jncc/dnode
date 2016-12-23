import json

def createList(outputFile):
# - Log into the api
# - Get Products list from api list_products
# - Check list against catalog 
# - Subtract downloded products from list
# - Create todays-date/available.json in lugi s3 folder

    productList = {
        "products" : ['test']
        }

    outputFile.write(json.dumps(productList))