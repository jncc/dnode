import requests
import hashlib
import zipfile
import shutil
import os
import pprint
import logging

class DatahubClient:
    """ This class demonstrates how to interact with the EnvSys datahub api.
        It prints out the json responses it receieves for each request from the API for clarity.
    """
    def __init__(self, base_url, download_chunk_size, search_zone, username, password, logger):
        """ Main constructor, initialises the class

        :param search_zone: ID of the search zone (this will be provided by ESL)
        :param username: Your username to connect to the API (provided by ESL)
        :param password: Your password to authorise the user (provided by ESL)
        """
        self.search_zone_id = search_zone
        self.session = requests.Session()   # Establish a session, saves authentication cookie...
        self.username = username
        self.password = password
        self.base_url = base_url
        self.download_chunk_size = download_chunk_size
        self.login_state_text = self.login()

    def check_response(self, response):
        """ Convenience method to check http responses.
            It is good practice to ensure we are getting valid responses before trying to parse them!
        """
        assert response.status_code == 200, "Got unexpected HTTP status code: {}".format(response.status_code)

    def login(self):
        """ Perform login - use a session to persist session cookie across requests.
            Login must be performed and stored in session before any other requests will work.
        """
        response = self.session.post(self.base_url + '/login', data={'username': self.username, 'password': self.password})

        self.check_response(response)
        parsed_json = response.json()   # python requests library includes json parseing...

        if not parsed_json['logged_in']:
            raise RuntimeError("Login failure: {}".format(parsed_json['login_state_text']))
        return parsed_json['login_state_text']

    def get_product_list(self):
        """ Queries the API for the list of currently available products.
            Parses the response as JSON and returns information about available products in a dict.

        :return: A list of dicts containing information about the available products.
        """
        response = self.session.get(self.base_url + '/list_products/{}'.format(self.search_zone_id))

        self.check_response(response)
        parsed_json = response.json()

        product_list = parsed_json['available_products']
        return product_list

    def download_product(self, product_id, filename, chunk_size=None):
        """ Downloads a product to a location on the hard disk. Shows the use of 'get_url' api function.
            It is a large file, therefore download should be managed carefully, downloading in chunks so
            as not to overload memory. NB: Use of the HTTP Range header (to resume a download)
            is supported by S3, but not shown here.

        :param product_id: The id of the product to download, taken from the product list dict
        :param filename: The location on disk where to store the downloaded file
        :param chunk_size: (Optional) The chunk size to use when downloading
        """
        if chunk_size is None:
            chunk_size = self.download_chunk_size

        response = self.session.get(self.base_url + '/get_url/{}'.format(product_id))

        self.check_response(response)
        parsed_json = response.json()

        url = parsed_json['url']
        with open(filename, 'wb+') as file_handle:
            # Download the full product archive
            stream = requests.get(url, stream=True)
            for chunk in stream.iter_content(chunk_size=4096):
                if chunk:
                    file_handle.write(chunk)

    def get_checksum(self, product_id):
        """ Gets the checksum for a product from the API and returns it. NB: The checksum
            applies to the tif data contained within the product, rather than the full
            product archive.

        :param product_id: The id of the product, as returned from list_products api call
        :return: The parsed checksum for the product
        """
        response = self.session.get(self.base_url + '/get_checksum/{}'.format(product_id))

        self.check_response(response)
        parsed_json = response.json()
        return parsed_json['checksum']

    def get_metadata(self, product_id):
        """ Gets the metadata entry for a product from the datahub API

        :param product_id: The ID of the product to pull metadata for
        :return: The XML metadata, as a string
        """
        response = self.session.get(self.base_url + '/get_metadata/{}'.format(product_id))
        self.check_response(response)
        return response.content

    def download_metadata(self, product_id, filename):
        """ Similar to get_metadata but downloads the metadata into a filename provided

        :param product_id: The product ID for which to pull metadata
        :param filename: The location on the filesystem where meteadata should be downloaded
        """
        response = self.session.get(self.base_url + '/get_metadata/{}'.format(product_id), stream=True)
        self.check_response(response)
        with open(filename, 'w+') as metafile:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, metafile)
