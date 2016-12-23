"""
Sample code demonstrating the use of the datahub api provided by Environment Systems Ltd.

This program will show all the functions available from the API, and print out the resultant
JSON from every request to act as documentation. The provided class is designed to act as a
reference implementation, rather than be used directly.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Author: Sebastian Clarke
(c) Environment Systems 2016
"""
import requests
import hashlib
import zipfile
import shutil
import os
import pprint

BASE_URL = 'https://datahub-dev.envsys.co.uk/api'
SEARCH_ZONE_ID = 1
DOWNLOAD_CHUNK_SIZE = 4096

JNCC_USER = 'jncc'
JNCC_PASS = 'CatdaydJebDiawl'

PP = pprint.PrettyPrinter(indent=4)


def check_response(response):
    """ Convenience method to check http responses.
        It is good practice to ensure we are getting valid responses before trying to parse them!
    """
    assert response.status_code == 200, "Got unexpected HTTP status code: {}".format(response.status_code)


def calculate_checksum(filename):
    """ Convenience demonstration function showing how to calculate an MD5 of a file within
        the downloaded dataset archive without first extracting it to the filesystem.

    :param filename: The filename of the downloaded dataset
    :return: The checksum of the tif data within the dataset archive
    """
    hasher = hashlib.md5()
    basename = os.path.basename(filename)
    with zipfile.ZipFile(filename, 'r') as product_zip:
        tif_file = product_zip.open('{}/{}'.format(basename, basename.replace('.SAFE.data', '.tif')))
        for chunk in iter(lambda: tif_file.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


class DatahubClient:
    """ This class demonstrates how to interact with the EnvSys datahub api.
        It prints out the json responses it receieves for each request from the API for clarity.
    """
    def __init__(self, search_zone, username, password):
        """ Main constructor, initialises the class

        :param search_zone: ID of the search zone (this will be provided by ESL)
        :param username: Your username to connect to the API (provided by ESL)
        :param password: Your password to authorise the user (provided by ESL)
        """
        self.search_zone_id = search_zone
        self.session = requests.Session()   # Establish a session, saves authentication cookie...
        self.username = username
        self.password = password
        self.login_state_text = self.login()

    def login(self):
        """ Perform login - use a session to persist session cookie across requests.
            Login must be performed and stored in session before any other requests will work.
        """
        response = self.session.post(BASE_URL + '/login', data={'username': self.username, 'password': self.password})
        print("=== Raw login Response ===")
        PP.pprint(response.content)
        print("==========================\n")



        check_response(response)
        parsed_json = response.json()   # python requests library includes json parseing...

        if not parsed_json['logged_in']:
            raise RuntimeError("Login failure: {}".format(parsed_json['login_state_text']))
        return parsed_json['login_state_text']

    def get_product_list(self):
        """ Queries the API for the list of currently available products.
            Parses the response as JSON and returns information about available products in a dict.

        :return: A list of dicts containing information about the available products.
        """
        response = self.session.get(BASE_URL + '/list_products/{}'.format(self.search_zone_id))
        print("=== Raw list_product Response ===")
        PP.pprint(response.content)
        print("=================================\n")

        check_response(response)
        parsed_json = response.json()


        product_list = parsed_json['available_products']
        return product_list

    def download_product(self, product_id, filename, chunk_size=DOWNLOAD_CHUNK_SIZE):
        """ Downloads a product to a location on the hard disk. Shows the use of 'get_url' api function.
            It is a large file, therefore download should be managed carefully, downloading in chunks so
            as not to overload memory. NB: Use of the HTTP Range header (to resume a download)
            is supported by S3, but not shown here.

        :param product_id: The id of the product to download, taken from the product list dict
        :param filename: The location on disk where to store the downloaded file
        :param chunk_size: (Optional) The chunk size to use when downloading
        """
        response = self.session.get(BASE_URL + '/get_url/{}'.format(product_id))
        print("=== Raw get_url Response ===")
        PP.pprint(response.content)
        print("============================\n")

        check_response(response)
        parsed_json = response.json()

        url = parsed_json['url']
        with open(filename, 'w+') as file_handle:
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
        response = self.session.get(BASE_URL + '/get_checksum/{}'.format(product_id))
        print("=== Raw get_checksum Response ===")
        PP.pprint(response.content)
        print("=================================\n")

        check_response(response)
        parsed_json = response.json()
        return parsed_json['checksum']

    def get_metadata(self, product_id):
        """ Gets the metadata entry for a product from the datahub API

        :param product_id: The ID of the product to pull metadata for
        :return: The XML metadata, as a string
        """
        response = self.session.get(BASE_URL + '/get_metadata/{}'.format(product_id))
        check_response(response)
        return response.content

    def download_metadata(self, product_id, filename):
        """ Similar to get_metadata but downloads the metadata into a filename provided

        :param product_id: The product ID for which to pull metadata
        :param filename: The location on the filesystem where meteadata should be downloaded
        """
        response = self.session.get(BASE_URL + '/get_metadata/{}'.format(product_id), stream=True)
        check_response(response)
        with open(filename, 'w+') as metafile:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, metafile)


def main():
    """ Main Function - demonstrates the use of the above class and runs a test with it. """
    # Set up the connection and login
    client = DatahubClient(SEARCH_ZONE_ID, JNCC_USER, JNCC_PASS)

    print("{}\n".format(client.login_state_text))

    # Get a list of available products
    product_list = client.get_product_list()

    # Print out some info about the products
    for product in product_list:
        print("Found product {} with id {}\n".format(product['filename'], product['product_id']))

    # Download the first product from the list
    to_download = product_list[0]
    download_location = '/home/felix/Development/Misc/s1_ard_api/data'  # just download to /tmp for this demonstration
    downloaded_dataset_filename = os.path.join(download_location, to_download['filename'])

    print("Downloading {} to {}\n".format(to_download['filename'], downloaded_dataset_filename))
    client.download_product(to_download['product_id'], downloaded_dataset_filename)

    # Get the checksum for the product from the API
    remote_checksum = client.get_checksum(to_download['product_id'])
    # Check the checksum
    local_checksum = calculate_checksum(downloaded_dataset_filename)
    if local_checksum == remote_checksum:
        print("File downloaded OK")
    else:
        print("Problem downloading file: Checksums do not match!")

    # Print the metadata
    print("Displaying metadata...")
    print(client.get_metadata(to_download['product_id']))

    # Save the metadata as a file
    metadata_download_location = os.path.join(download_location, "metadata.xml")
    print("Downloading metadata to {}".format(metadata_download_location))
    client.download_metadata(to_download['product_id'], metadata_download_location)

    print("Done!")

if __name__ == "__main__":
    main()
