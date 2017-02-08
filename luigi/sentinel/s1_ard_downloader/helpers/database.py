import json, uuid

"""
Write the progress of this download to the database (i.e. failure, etc...)

:param db_conn: A Connection to the DB
:param collection_version_uuid: A UUID for the collection version
:param item: The item that we want to record progress against (item pulled from API)
:param representations: The files that we have uploaded to S3 and some basic metadata about them
:param success: If the download / upload was successfull or not
:param additional: Any additional metadata that we need (realtedTo uuid for OSNI uploads)
:param geom: GeoJSON represenation of the footprint of the data we are recording progress against
"""
def write_progress_to_database(db_conn, collection_version_uuid, item, metadata, representations, geom, additional=None):
    cur = db_conn.cursor()

    # if product_id in item:
    #     cur.execute("SELECT properties->>'product_id' FROM sentinel_ard_backscatter WHERE properties->>'product_id' = %s;", (item['product_id']),)
    #     existing = cur.fetchone()
    # else:
    #     existing = None

    retVal = None

    if 'ID' in metadata:
        # Grab the UUID from the metadata if possible, if not create one
        uuid_str = metadata['ID']
        try:
            val = uuid.UUID(uuid_str, version=4)
        except ValueError:
            uuid_str = str(uuid.uuid4())
            metadata['ID'] = uuid_str
    else:
        # Possibly blank metadata, success is very false here so set it that way
        success = False
        uuid_str = str(uuid.uuid4())
        metadata['ID'] = uuid_str

    # If UUID is equal to the optionally provided relatedTo UUID then generate a new one and replace the 
    # one in the metadata with it 
    if additional is not None:
        props['relatedTo'] = additional['relatedTo']
        # Catch to replace non unique uuid's
        if metadata['ID'] == additional['relatedTo']:
            uuid_str = str(uuid.uuid4())
            metadata['ID'] = uuid_str

    # if existing is not None or additional is not None:
    #     # Entry exists
    #     if existing is not None:
    #         props = json.loads(existing[3])
    #     else:
    #         props = {}

    #     if geom is None:
    #         if additional is not None:
    #             # If we are adding an extra record with the same ID i.e. OSNI projection
    #             props['relatedTo'] = additional['relatedTo']
    #             cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, %s, %s, %s, null)", (uuid_str,
    #                 collection_version_uuid, json.dumps(metadata), json.dumps(props), json.dumps(representations), geom, ))
    #             retVal = cur.fetchone()[0]
    #         else:
    #             cur.execute("UPDATE sentinel_ard_backscatter SET properties = %s, representations = %s, footprint = null WHERE id = %s", (
    #                 json.dumps(props), json.dumps(representations), geom, existing(0), ))
    #     else:
    #         if additional is not None:
    #             # If we are adding an extra record with the same ID i.e. OSNI projection
    #             props['relatedTo'] = additional['relatedTo']
    #             cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, %s, %s, %s, ST_Multi(ST_GeomFromGeoJSON(%s)))", (uuid_str, 
    #                 collection_version_uuid, json.dumps(metadata), json.dumps(props), json.dumps(representations), geom, ))
    #             retVal = cur.fetchone()[0]
    #         else:
    #             cur.execute("UPDATE sentinel_ard_backscatter SET properties = %s, representations = %s, footprint = ST_GeomFromGeoJSON(%s) WHERE id = %s", (
    #                 json.dumps(props), json.dumps(representations), geom, existing(0), ))
                
    #             retVal = existing(0)

    # Entry does not exist
    props = item

    if geom is None:
        cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, %s, %s, %s, null)", (uuid_str,
            collection_version_uuid, json.dumps(metadata), json.dumps(props), json.dumps(representations), ))
    else:
        cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, %s, %s, %s, ST_Multi(ST_GeomFromGeoJSON(%s)))", (uuid_str,
            collection_version_uuid, json.dumps(metadata), json.dumps(props), json.dumps(representations), json.dumps(geom), ))
    
    # Commit
    db_conn.commit()

    cur.close()
    return uuid_str