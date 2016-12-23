# Create new products list
Creates:
- available.json

- Log into the api
- Get Products list from api list_products
- Check list against catalog 
- Subtract downloded products from list
- Create todays-date/available.json in lugi s3 folder

# Download new products
Requires:
- avialable.json
Creates:
- downloaded.json

- Read available.json
- Log into the api
- for each product in available products list
	- Download product to temporary area
	- Get checksum for product from api get_checksum 
	- if checksum ok
		Move file to s3 repository for s1 ard products
		write product id to downloded products list
	  else
	  	Discard
		log?
	- Write out 

# Get metadata for downloaded products
Requires
- downloaded.json
Creates:
- _success.json

- read downloaded.json
- Log into api
- for each product in downloaded prodcuts list
	-Request gemini metadata from get_metadata
	-Write metadata to catalog
		-Prduction catalog could error if not valid gemini deal with this.
- Create _success.json