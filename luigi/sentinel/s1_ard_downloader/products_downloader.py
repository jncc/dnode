import json

def downloadProducts(available, downloaded):
    available_list = json.load(available)
    downloaded.write(json.dumps(available_list))

    # - Read available.json
    # - Log into the api
    # - for each product in available products list
    # 	- Download product to temporary area
    # 	- Get checksum for product from api get_checksum 
    # 	- if checksum ok
    # 		Move file to s3 repository for s1 ard products
    # 		write product id to downloded products list
    # 	  else
    # 	  	Discard
    # 		log?
    # 	- Write out 