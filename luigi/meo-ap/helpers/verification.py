import hashlib

""" 
Calculate checksum of a given file

:param filename: The filename of the downloaded dataset
:return: The checksum of the file specifed by the filename
"""
def calculate_checksum(filename):
    hasher = hashlib.md5()
    with open(filename, 'rb') as stream:
        for chunk in iter(lambda: stream.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()