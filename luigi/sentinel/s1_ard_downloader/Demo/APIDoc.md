ESL Datahub API
===============

This document briefly describes the functions available in the Environment Systems datahub API for receiving processed Sentinel imagery. The API is provided with an example script, showing its usage, which this document is meant to complement. The API is HTTP/JSON based, requests are submitted over HTTP, responses come back as JSON.

Function: Login
---------------
**URL:** ${datahub_api_url}/login

**Method:** POST

**Post Parameters:** username, password

**Sample Response:** {"logged_in": true, "login_state_text": "You're successfully logged in!"}

**Description:** This function authenticates you and establishes a session with the server, it must be called prior to any subsequent requests. The returned JSON contains two top level elements, "logged_in" and "login_state_text". The first encodes whether the login request was successful, the second provides a status text response from the server.


Function: List Products
-----------------------
**URL:** ${datahub_api_url}/list_products/${zone_id}

**Method:** GET

**Sample Request:** https://datahub-dev.envsys.co.uk/api/list_products/1

**Sample Response:** {"available_products": [{"product_id": 4, "filename": "S1A_IW_GRDH_1SSV_20160817T105358_20160817T105423_012639_013D5D_68E8.SAFE.data"}]}

**Description:** This function is used to retrieve the currently available list of products for your zone (a valid zone_id for your usage will be provided by ESL). The returned JSON contains a single top-level element ("available_products"), which contains a list with an entry per avaialable product.
Each entry in the list contains two keys: "product_id" - an id used for simple further referencing of the product; "filename" - the filename of the archive,  containing the tiff data and supplementary information.


Function: Get URL
-----------------------
**URL:** ${datahub_api_url}/get_url/${product_id}

**Method:** GET

**Sample Request:** https://datahub-dev.envsys.co.uk/api/get_url/4

**Sample Response:** {"url": "https://topmets-test.s3.amazonaws.com/S1A_IW_GRDH_1SSV_20160817T105358_20160817T105423_012639_013D5D_68E8.SAFE.data?AWSAccessKeyId=AKIAI4IW2UBLAX5GZ54Q&Expires=1480330922&Signature=O%2BH0GrwnEco2vI0LER6wrN92FwA%3D", "product_id": 4, "filename": "S1A_IW_GRDH_1SSV_20160817T105358_20160817T105423_012639_013D5D_68E8.SAFE.data"}

**Description:** This function is used to request a temporary download url, valid for 1 hour, allowing you to download the product from S3. If you are not able to download the product in this time, you will have to resubmit this request to get another download link. The request is performed using a product_id, as returned by the 'list_products' function. The response JSON contains 3 top-level elements: "url" - the provided download url from where the product archive can be pulled; "product_id" - the product id used in the request and which this download link is for; "filename" - archive filename of the product, save the product as this name to avoid confusion.


Function: Get Checksum
-----------------------
**URL:** ${datahub_api_url}/get_checksum/${product_id}

**Method:** GET

**Sample Request:** https://datahub-dev.envsys.co.uk/api/get_checksum/4

**Sample Response:** {"checksum": "f5966805c6d5556192107a923338946e", "product_id": 4}

**Description:** This function is used to retrieve the computed checksum for a product. Checksums are computed as an MD5 digest. This function is called with a product id as returned by the 'list_products' function. The response JSON contains two top-level elements: "checksum" - the computed checksum for the product; "product_id" - the ID of the product for which the computed checksum is valid. **NB: Checksums are computed for the tiff file within the product archive only, not the archive itself. In order to affirm the correct download of a file archive, the tiff file should first be extracted and an MD5 checksum calculated on it, which should match that returned by this function**


Function: Get Metadata
-----------------------
**URL:** ${datahub_api_url}/get_metadata/${product_id}

**Method:** GET

**Sample Request:** https://datahub-dev.envsys.co.uk/api/get_metadata/4

**Sample Response:** <?xml version="1.0" encoding=...

**Description:** This function is used to retrieve the verified Gemini compliant metadata for a product. The function is called using the id of the desired product, as returned by the 'list_products' function. The metadata is returned direct as an XML (rather than JSON) response which can be parsed in place or streamed to a file.


