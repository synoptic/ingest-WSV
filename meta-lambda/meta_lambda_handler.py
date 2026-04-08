import requests
import json
import logging
import time
import os
import posixpath
import math

from ingestlib import aws, station_lookup, core

########################################################################################################################
# OVERVIEW
########################################################################################################################
# germanyWSV Metadata Ingest
#
# Features:
# - Fetches station metadata from Pegelonline REST API
# - Validates lat/lon
# - Applies DWD exact match mapping
# - Generates station lookup payload
# - Saves metadata locally and uploads to S3
# - Runs station_lookup.load_metamgr
########################################################################################################################

########################################################################################################################
# DEFINE CONSTANTS
########################################################################################################################
INGEST_NAME = "germanyWSV"
M_TO_FEET = 3.28084
ELEVATION_UNIT = 'METERS' # ELEVATION UNIT OF THIS INGESTS METADATA MUST BE EITHER 'METERS' OR 'FEET'. METAMOTH CURRENTLY STORES ELEVATION IN FEET, SO WE WILL CONVERT IF IT'S IN METERS. 
PEGELONLINE_BASE = "https://www.pegelonline.wsv.de/webservices/rest-api/v2"
STATIONS_URL = f"{PEGELONLINE_BASE}/stations.json?includeTimeseries=true"
MNET_ID = 343
MNET_SHORTNAME = "WSV"
RESTRICTED_DATA_STATUS = False
RESTRICTED_METADATA_STATUS = False
STID_PREFIX = "WSV"

########################################################################################################################
# DWD EXACT MATCH MAP
########################################################################################################################

EXACT_MATCH_DWD_MAP = {
    "CELLE": "DWD10343",
    "KONSTANZ": "DWD10929",
    "WITTENBERG": "DWD10474",
    "BOIZENBURG": "DWD10249",
    "HETLINGEN": "DWDR278",
    "GENTHIN": "DWD10365",
    "UELZEN": "DWDE475",
    "PAPENBURG": "DWDR386",
    "FRIEDRICHSTHAL": "DWDS701",
    "RAUNHEIM": "DWDL829",
    "BAMBERG": "DWD10675",
    "RECKE": "DWDT123",
    "BRAMSCHE": "DWDR519",
    "LIST AUF SYLT": "DWD10020",
    "BARTH": "DWD10180",
    "STRALSUND": "DWDS050",
    "WOLGAST": "DWDS189",
    "KARLSHAGEN": "DWDB382",
    "KIEL-HOLTENAU": "DWD10046",
    "SCHLESWIG": "DWD10035",
    "DEMMIN": "DWDS220",
    "ANKLAM": "DWDB488",
    "POTSDAM": "DWD10379",
    "MANNHEIM": "DWD10729",
    "WORMS": "DWDK699",
    "ANDERNACH": "DWD10520",
    "BRAKE": "DWDE235",
    "PETERSHAGEN": "DWDH027",
    "NIENBURG": "DWDE652",
}

########################################################################################################################
# LOGGING
########################################################################################################################
logger = logging.getLogger(f"{INGEST_NAME}_ingest")

########################################################################################################################
# DEFINE ETL/PARSING FUNCTIONS
########################################################################################################################

def generate_metadata_payload(station_meta, payload_type, source_info=None):
    """
    Generates the metadata payload for ingestlib station lookup

    Args:
        station_meta (dict): A dictionary containing station metadata.
        payload_type (str): Type of payload ('station_lookup' or 'metamanager').
        source_info (dict): Optional source information for the metamanager payload.

    Returns:
        dict or str: Parsed metadata payload based on the payload type.
    """
    if payload_type not in {"station_lookup", "metamanager"}:
        raise ValueError("Invalid payload_type. Must be 'station_lookup' or 'metamanager'.")

    metadata = []

    for station_id, row in station_meta.items():
        try:
            # Extract required fields from the row
            stid = row.get('SYNOPTIC_STID', None)
            name = row.get('NAME', None)
            lat = row.get('LAT', None)
            lon = row.get('LON', None)
            otid = row.get('OTID', None)
            elevation = row.get('ELEVATION', None)


            # Clean name with ascii characters, NOTE that we are NOT converting single apostrophe's to double apostrophe's
            # station_lookup.load_metamgr does this already. Duplicating the apostrophe's is unnecessary
            if name:
                clean_name = core.ascii_sanitize(name) if not name.isascii() else name
            else:
                clean_name = None

            # Check lat/lon validity
            if lat is None or lon is None:
                continue
            lat = float(lat)
            lon = float(lon)
            if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
                logger.debug(f"Skipping station {station_id} due to invalid lat/lon: {lat}, {lon}")
                continue

            # Check Elevation
            if elevation is not None:
                elevation = float(elevation)
                
                if ELEVATION_UNIT == 'METERS':
                    elevation *= M_TO_FEET
                elif ELEVATION_UNIT != 'FEET':
                    raise ValueError("Invalid ELEVATION_UNIT, must be 'METERS' or 'FEET'")
                
                if math.isnan(elevation):
                    elevation = None

            if stid and name:
                station = {
                    "STID": stid,
                    "NAME": clean_name,
                    "LATITUDE": lat,
                    "LONGITUDE": lon,
                    "OTHER_ID": otid,
                    "MNET_ID": MNET_ID,
                    "ELEVATION": None if elevation is None else round(elevation, 3),
                    "RESTRICTED_DATA": row.get('RESTRICTED_DATA', RESTRICTED_DATA_STATUS),
                    "RESTRICTED_METADATA": row.get('RESTRICTED_METADATA', RESTRICTED_METADATA_STATUS)
                }
                metadata.append(station)
            else:
                logger.debug(f"Skipping station {station_id} due to missing required fields: STID or NAME.")
        except ValueError as e:
            logger.debug(f"Skipping station {station_id} due to error: {e}")

    if payload_type == "station_lookup":
        payload = {
            "MNET_ID": MNET_ID,
            "STNS": metadata
        }
    else:
        default_source = {
            "name": "Administration Console",
            "environment": str(MNET_ID)
        }
        payload = {
            "source": source_info if source_info else default_source,
            "metadata": metadata
        }
    
    return json.dumps(payload, indent=4) if payload_type == "metamanager" else payload


def update_stations(url: str, headers: dict, payload: str) -> requests.Response:
    """
    Sends a PUT request to update station data.

    Args:
        url (str): The URL to send the request to.
        headers (dict): The headers to include in the request.
        payload (str): The payload data in JSON format.

    Returns:
        requests.Response: The response from the server.
    """
    response = requests.request("PUT", url, headers=headers, data=payload)
    return response

# Function to save data to a JSON file
def save_to_json(data, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

########################################################################################################################
# FETCH PEGELONLINE
########################################################################################################################

def fetch_pegelonline_stations():
    logger.debug("Fetching Pegelonline stations")
    response = requests.get(STATIONS_URL, timeout=60)
    response.raise_for_status()
    stations = response.json()
    logger.debug(f"Fetched {len(stations)} stations")
    return stations

########################################################################################################################
# PARSE STATIONS
########################################################################################################################

def build_station_meta(raw_stations):
    station_meta = {}
    for stn in raw_stations:
        uuid = stn.get("uuid")
        number = stn.get("number")
        shortname = stn.get("shortname", "")
        longname = stn.get("longname", shortname)
        lat = stn.get("latitude")
        lon = stn.get("longitude")
        if not uuid or lat is None or lon is None:
            continue
        lat = float(lat)
        lon = float(lon)
        if lat == 0 and lon == 0:
            continue
        stid = f"{STID_PREFIX}{str(number).zfill(10)}"
        short_upper = shortname.upper().strip()
        other_id = EXACT_MATCH_DWD_MAP.get(short_upper, number)
        station_meta[uuid] = {
            "SYNOPTIC_STID": stid,
            "NAME": longname,
            "LAT": lat,
            "LON": lon,
            "OTID": other_id,
            "RESTRICTED_DATA": RESTRICTED_DATA_STATUS,
            "RESTRICTED_METADATA": RESTRICTED_METADATA_STATUS
        }
    logger.debug(f"Parsed {len(station_meta)} stations")
    return station_meta

def fetch_and_build_metadata():
    """
    Fetches raw station data and builds station metadata.
    """
    try:
        raw_stations = fetch_pegelonline_stations()
        station_meta = build_station_meta(raw_stations)
        return station_meta
    except Exception as e:
        logger.exception("Error fetching or building station metadata")
        raise

########################################################################################################################
# MAIN FUNCTION
########################################################################################################################
def main(event, context):
    from args import args

    # Directories
    work_dir = log_dir = s3_work_dir = None
    if args.local_run or args.dev:
        log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dev"))
        work_dir = "../dev/"
        s3_work_dir = "metadata/"
    else:
        log_dir = "/tmp/tmp/"
        work_dir = "/tmp/tmp/"
        s3_work_dir = "metadata/"
    os.makedirs(work_dir, exist_ok=True)

    # Logging
    log_file = core.setup_logging(
        logger, INGEST_NAME,
        log_level=getattr(args, "log_level", "INFO"),
        write_logs=True,
        log_dir=log_dir,
        filename=f"{INGEST_NAME}_meta.log"
    )
    core.setup_signal_handler(logger, args)
    logger.debug(vars(args))
    start_runtime = time.time()

    try:
        # S3 config
        s3_bucket_name = os.environ.get('INTERNAL_BUCKET_NAME')
        if not s3_bucket_name:
            raise ValueError("Missing INTERNAL_BUCKET_NAME env var.")
        s3_station_meta_file = posixpath.join(s3_work_dir, f"{INGEST_NAME}_stations_metadata.json")
        station_meta_file = os.path.join(work_dir, f"{INGEST_NAME}_stations_metadata.json")

        # Load existing stations
        existing_stations = {}
        try:
            aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_station_meta_file, local_directory=work_dir)
            with open(station_meta_file, 'r') as f:
                existing_stations = json.load(f)
            logger.debug(f"Loaded {len(existing_stations)} existing stations")
        except FileNotFoundError:
            logger.debug("No existing station metadata found")
        except Exception as e:
            logger.warning(f"Failed to load existing station metadata: {e}")

        # Fetch stations
        station_meta = fetch_and_build_metadata()

        # Save locally in dev
        if args.local_run:
            dev_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dev"))
            os.makedirs(dev_dir, exist_ok=True)
            dev_path = os.path.join(dev_dir, f"{INGEST_NAME}_stations_metadata.json")
            save_to_json(station_meta, dev_path)
            logger.debug(f"[DEV] Saved station_meta to {dev_path}")
            logger.debug(f"[DEV] Station count: {len(station_meta)}")

        # Station lookup payload
        station_lookup_payload = generate_metadata_payload(station_meta, payload_type='station_lookup')

        # Production: station lookup + S3 upload
        if not args.local_run:
            logger.debug("Running station lookup in production mode")
            try:
                station_lookup.load_metamgr(station_lookup_payload, mode='prod', logstream=logger, output_location=work_dir)
                logger.debug("Station lookup completed successfully")
            except Exception as e:
                logger.exception(f"Station lookup failed: {e}")
                raise

            # Upload metadata
            save_to_json(station_meta, station_meta_file)
            aws.S3.upload_file(local_file_path=station_meta_file, bucket=s3_bucket_name, s3_key=s3_station_meta_file)
            logger.debug(f"Saved {len(station_meta)} stations to {s3_station_meta_file}")

            # Cleanup old SQL files
            deleted_count = aws.S3.delete_files(bucket=s3_bucket_name, prefix=s3_work_dir, endswith=".sql")
            logger.debug(f"Deleted {deleted_count} SQL files from {s3_bucket_name}")
            for f in os.listdir(work_dir):
                if f.endswith(".sql"):
                    s3_key = f"{s3_work_dir}/{f}"
                    aws.S3.upload_file(local_file_path=os.path.join(work_dir, f), bucket=s3_bucket_name, s3_key=s3_key)

        logger.info(json.dumps({"completion": 1, "time": time.time() - start_runtime}))

    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
        logger.error(json.dumps({"completion": 0, "time": time.time() - start_runtime}))

    finally:
        total_runtime = time.time() - start_runtime
        logger.debug(f"Total execution time: {total_runtime:.2f} seconds")
        logging.shutdown()
