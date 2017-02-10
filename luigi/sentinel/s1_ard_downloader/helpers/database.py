import json, uuid

"""
Write the progress of this download to the database (i.e. failure, etc...)

:param db_conn: A Connection to the DB
:param collection_version_uuid: A UUID for the collection version
:param item: The item that we want to record progress against (item pulled from API)
:param representations: The files that we have uploaded to S3 and some basic metadata about them
:param additional: Any additional metadata that we need (realtedTo uuid for OSNI uploads)
:param geom: GeoJSON represenation of the footprint of the data we are recording progress against
"""
def write_progress_to_database(db_conn, collection_version_uuid, item, metadata, representations, geom, additional):
    cur = db_conn.cursor()

    # if product_id in item:
    #     cur.execute("SELECT properties->>'product_id' FROM sentinel_ard_backscatter WHERE properties->>'product_id' = %s;", (item['product_id']),)
    #     existing = cur.fetchone()
    # else:
    #     existing = None

    if 'ID' in metadata:
        # Grab the UUID from the metadata if possible, if its not valid create one
        uuid_str = metadata['ID']
        try:
            val = uuid.UUID(uuid_str, version=4)
        except ValueError:
            uuid_str = str(uuid.uuid4())
            metadata['ID'] = uuid_str
    else:
        # Missing ID, so generate one for now and save it in the metadata blob
        uuid_str = str(uuid.uuid4())
        metadata['ID'] = uuid_str

    # Entry does not exist
    props = item

    if additional is not None:
        # Copy any additional metadata for this product to the properties field
        for akey in additional.keys():
            props[akey] = additional[akey]
        # Catch to replace non unique uuid's if they come through from related field
        if 'relatedTo' in props and metadata['ID'] == props['relatedTo']:
            uuid_str = str(uuid.uuid4())
            metadata['ID'] = uuid_str

    if geom is None:
        cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, %s, %s, %s, null)", (uuid_str,
            collection_version_uuid, json.dumps(metadata), json.dumps(props), json.dumps(representations), ))
    else:
        if 'crs' not in geom:
            ## Add a CRS to the data if none exists, so assume 4326
            geom['crs'] = { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4326" } }
        cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, %s, %s, %s, ST_Force2D(ST_Multi(ST_GeomFromGeoJSON(%s))))", (uuid_str,
            collection_version_uuid, json.dumps(metadata), json.dumps(props), json.dumps(representations), json.dumps(geom), ))
    
    # Commit
    db_conn.commit()

    cur.close()
    return uuid_str