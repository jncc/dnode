import json

def getProductMetadata(downloaded, success):
    
    downloaded_list = json.load(downloaded)
    success.write(json.dumps(downloaded_list))