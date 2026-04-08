import logging
import os
import time
import json
import posixpath
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from functools import partial

from ingestlib import poe, parse, aws, validator, metamgr, core
from data_dictionary import variables

########################################################################################################################
# DEFINE LOGSTREAMS AND CONSTANTS
########################################################################################################################
INGEST_NAME = "germanyWSV"
logger = logging.getLogger(f"{INGEST_NAME}_ingest")

# How many hours of seen_obs we keep locally to suppress re-sends.
PREVIOUS_HOURS_TO_RETAIN = 12

PEGELONLINE_STATIONS_URL = (
    "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    "?includeTimeseries=true"
    "&includeCurrentMeasurement=true"
)

########################################################################################################################
# FETCH — retrieve all current measurements in one call
########################################################################################################################

def fetch_pegelonline_data():

    logger.info(f"FETCH: GET {PEGELONLINE_STATIONS_URL}")
    try:
        resp = requests.get(PEGELONLINE_STATIONS_URL, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"FETCH FAILED: {e}")
        return []

    try:
        data = resp.json()
    except ValueError as e:
        logger.error(f"JSON decode error: {e}")
        return []

    logger.info(f"FETCH: received {len(data)} station records")
    return data


########################################################################################################################
# DEFINE ETL/PARSING FUNCTIONS
########################################################################################################################
def cache_raw_data_simple(incoming_data, work_dir: str, s3_bucket_name: str, s3_prefix: str):
    try:
        if not incoming_data:
            logger.debug("CACHE: no incoming data; skipping")
            return False

        # Create timestamp-based filename
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        # Create S3 path: prefix/YYYY/MM/YYYYMMDD_HHMMSS.json
        year_month = datetime.now(timezone.utc).strftime('%Y/%m')
        s3_key = f"{s3_prefix}/{year_month}/{timestamp}.json"
        
        # Local file setup
        os.makedirs(work_dir, exist_ok=True)
        local_file_path = os.path.join(work_dir, f"{timestamp}.json")

        logger.info(f"CACHE: target s3://{s3_bucket_name}/{s3_key}")

        # Save data to local file
        try:
            with open(local_file_path, 'w', encoding='utf-8') as f:
                json.dump(incoming_data, f, indent=2, ensure_ascii=False)
            
            file_size = os.path.getsize(local_file_path)
            logger.debug(f"CACHE: created local file {local_file_path} ({file_size} bytes)")
            
        except Exception as e:
            logger.error(f"CACHE: failed to create local file: {e}")
            return False

        # Upload to S3
        t1 = time.time()
        try:
            logger.debug("CACHE: uploading to S3")
            aws.S3.upload_file(local_file_path, s3_bucket_name, s3_key)
            logger.info(f"CACHE: upload OK in {time.time()-t1:.2f}s; size={file_size}B")
            return True
            
        except Exception as e:
            logger.error(f"CACHE: failed to upload in {time.time()-t1:.2f}s: {e}")
            return False

    except Exception as e:
        logger.error(f"CACHE: unexpected error: {e}")
        return False

########################################################################################################################
# PARSE — build grouped_obs_set from raw API response
########################################################################################################################

def parse_pegelonline_data(raw_stations: list, station_meta: dict):
    grouped_obs_set = {}

    # Build a fast uuid → SYNOPTIC_STID reverse lookup
    uuid_to_stid = {
        uuid: meta.get("SYNOPTIC_STID", uuid)
        for uuid, meta in station_meta.items()
    }

    cutoff = datetime.now(timezone.utc) - timedelta(hours=PREVIOUS_HOURS_TO_RETAIN)

    skipped_stale = 0
    skipped_no_meta = 0
    skipped_bad_value = 0
    parsed_obs = 0

    for stn in raw_stations:
        uuid = stn.get("uuid")
        shortname_stn = stn.get("shortname", "UNKNOWN")

        # ------------------------------------------------------------------
        # Resolve SYNOPTIC STID — fall back to number-based ID if not in
        # metadata (station not yet provisioned by the meta lambda).
        # ------------------------------------------------------------------
        if uuid in uuid_to_stid:
            stid = uuid_to_stid[uuid]
        else:
            number = stn.get("number", "")
            try:
                stid = f"WSV{str(int(number)).zfill(10)}"
            except (TypeError, ValueError):
                stid = f"WSV{str(number).replace(' ', '').zfill(10)}"
            skipped_no_meta += 1
            logger.debug(
                f"UUID {uuid} ({shortname_stn}) not in station_meta; "
                f"using fallback STID {stid}"
            )

        # ------------------------------------------------------------------
        # Iterate over each variable timeseries this station reports
        # ------------------------------------------------------------------
        for ts in stn.get("timeseries", []):
            ts_short = ts.get("shortname", "")   # e.g. "W", "Q", "WT"

            # Skip variables not in our data dictionary
            if ts_short not in variables:
                continue

            current = ts.get("currentMeasurement")
            if not current:
                continue

            raw_ts = current.get("timestamp")
            raw_val = current.get("value")

            if raw_ts is None or raw_val is None:
                skipped_stale += 1
                continue

            try:
                dt_local = datetime.fromisoformat(raw_ts)
                dt_utc = dt_local.astimezone(timezone.utc)
            except Exception as e:
                logger.debug(
                    f"Timestamp parse failed for {shortname_stn}/{ts_short}: "
                    f"{raw_ts} — {e}"
                )
                skipped_bad_value += 1
                continue

            # Skip observations older than our retain window
            if dt_utc < cutoff:
                skipped_stale += 1
                continue

            obs_time_str = dt_utc.strftime("%Y%m%d%H%M")

            # Numeric cast
            try:
                raw_value = float(raw_val)
            except (TypeError, ValueError):
                logger.debug(
                    f"Non-numeric value for {shortname_stn}/{ts_short}: {raw_val}"
                )
                skipped_bad_value += 1
                continue

            var_def = variables[ts_short]
            vargem = var_def["vargem"]
            vnum = int(var_def["VNUM"])
            incoming_unit = parse.get_translated_value(
                ts_short, variables=variables, field="incoming_unit"
            )
            final_unit = var_def["final_unit"]

            if incoming_unit and incoming_unit != final_unit:
                try:
                    conversion_name = parse.create_conversion(
                        incoming_unit, variables, ts_short
                    )
                    final_value = round(
                        parse.convert_units(conversion_name, raw_value), 3
                    )
                except Exception:
                    final_value = round(raw_value, 3)
            else:
                final_value = round(raw_value, 3)

            # ----------------------------------------------------------
            # Build the observation key: STID|YYYYMMDDHHM
            # and insert into grouped_obs_set
            # ----------------------------------------------------------
            key = f"{stid}|{obs_time_str}"

            if key not in grouped_obs_set:
                grouped_obs_set[key] = {}

            if vargem in grouped_obs_set[key]:
                grouped_obs_set[key][vargem][vnum] = final_value
            else:
                grouped_obs_set[key][vargem] = {vnum: final_value}

            parsed_obs += 1

    return grouped_obs_set

# HELPERS

def filter_valid_seen_obs(seen_obs: list):
    valid = []
    for entry in seen_obs:
        if isinstance(entry, str) and entry.count("|") == 2:
            valid.append(entry)
        else:
            logger.warning(f"Dropping malformed seen_obs entry: {entry!r}")
    return valid


########################################################################################################################
# MAIN FUNCTION
########################################################################################################################

def main(event, context):
    from args import args

    # --- decide dirs once ---
    if args.local_run or args.dev:
        log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dev"))
        work_dir = "../dev/"
        s3_work_dir = "dev_tmp/"
    else:
        log_dir = "/tmp/tmp/"
        work_dir = "/tmp/tmp/"
        s3_work_dir = "tmp/"

    os.makedirs(work_dir, exist_ok=True)

    # --- logging (stdout + single file; overwrites each run) ---
    log_file = core.setup_logging(
        logger, INGEST_NAME,
        log_level=getattr(args, "log_level", "INFO"),
        write_logs=True,
        log_dir=log_dir,                         # where the file lives (dev or prod)
        filename=f"{INGEST_NAME}_obs.log",       # stable name for S3 overwrite
    )

    # --- signals ---
    core.setup_signal_handler(logger, args)

    logger.debug(f"poe socket: {args.poe_socket_address}")
    logger.debug(f"poe socket port: {args.poe_socket_port}")

    start_runtime = time.time()
    success_flag = 0

    try:
        logger.info("BOOT: ECS logging path OK")

        # paths
        os.makedirs(work_dir, exist_ok=True)

        # S3 path declarations
        s3_bucket_name = os.environ["INTERNAL_BUCKET_NAME"]
        cache_s3_bucket_name = os.environ.get("CACHE_S3_BUCKET_NAME", "synoptic-ingest-provider-data-cache-a4fb6")

        s3_meta_work_dir = "metadata"
        s3_station_meta_file = posixpath.join(s3_meta_work_dir, f"{INGEST_NAME}_stations_metadata.json")
        s3_seen_obs_file   = posixpath.join(s3_work_dir, "seen_obs.txt")
        seen_obs_file      = os.path.join(work_dir, "seen_obs.txt")
        station_meta_file  = os.path.join(work_dir, f"{INGEST_NAME}_stations_metadata.json")

        # Download seen observations file
        data_archive_time = datetime.now(timezone.utc) - timedelta(hours=PREVIOUS_HOURS_TO_RETAIN)

        if not args.local_run:
            try:
                aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_seen_obs_file, local_directory=work_dir)
            except Exception as e:
                logger.warning(f"Could not download seen_obs: {e} - starting fresh")

            try:
                aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_station_meta_file, local_directory=work_dir)
            except Exception as e:
                logger.warning(f"Could not download station_meta: {e} - STID fallback will be used")
        else:
            logger.debug("LOCAL_RUN: skipping S3 downloads for seen_obs and station_meta")
            if os.path.exists(seen_obs_file):
                logger.debug(f"LOCAL_RUN: found local seen_obs at {seen_obs_file}")
            else:
                logger.debug("LOCAL_RUN: no local seen_obs found - all obs treated as new")
            if os.path.exists(station_meta_file):
                logger.debug(f"LOCAL_RUN: found local station_meta at {station_meta_file}")
            else:
                logger.debug("LOCAL_RUN: no local station_meta found - STID fallback will be used")

        ########################################################################################################################
        # GET LATEST OBS
        ########################################################################################################################

        # load station metadata file
        if os.path.exists(station_meta_file):
            station_meta = parse.load_json_file(file_path=station_meta_file)
            logger.info(f"Loaded station_meta with {len(station_meta)} entries")
        else:
            station_meta = {}
            logger.warning("station_meta is empty - all STIDs will use fallback generation")

        # FETCH — one HTTP call for the entire network
        logger.info("FETCH: requesting bulk data from Pegelonline API")
        incoming_data = fetch_pegelonline_data()
        logger.info(f"FETCH: got data? {bool(incoming_data)} ({len(incoming_data)} stations)")
        
        # store raw raw incoming data in the data provider raw cache bucket
        cache_raw_data_simple(
            incoming_data=incoming_data, 
            work_dir=work_dir, 
            s3_bucket_name=cache_s3_bucket_name, 
            s3_prefix=INGEST_NAME
        )

        
        if incoming_data:
            logger.info(msg=json.dumps({'Incoming_Data_Success': 1}))

        # ------------------------------------------------------------------
        # PARSE — convert raw stations → grouped observations
        # ------------------------------------------------------------------
        grouped_obs_set = parse_pegelonline_data(incoming_data, station_meta)
        logger.info(f"PARSE: {len(grouped_obs_set)} unique station-time keys")
        grouped_obs = ['|'.join([k, json.dumps(v)]).replace(' ', '') for k, v in grouped_obs_set.items()] 

        ########################################################################################################################
        # VALIDATE DATA
        ########################################################################################################################
        # save the grouped obs and station meta if it exists
        if args.local_run or args.dev:
            dev_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dev"))
            os.makedirs(dev_dir, exist_ok=True)

            # Save grouped_obs as a text file (one line per observation string)
            grouped_obs_path = os.path.join(dev_dir, 'grouped_obs.txt')
            with open(grouped_obs_path, 'w', encoding="utf-8") as f:
                for obs in grouped_obs:
                    f.write(obs + '\n')
            logger.debug(f"[DEV] Saved grouped_obs to {grouped_obs_path}")

            # Save station_meta if available
            if 'station_meta' in locals():
                station_meta_path = os.path.join(dev_dir, 'station_meta.json')
                with open(station_meta_path, 'w', encoding="utf-8") as f:
                    json.dump(station_meta, f, indent=4, ensure_ascii=False)
                logger.debug(f"[DEV] Saved station_meta to {station_meta_path}")

        if args.dev or args.local_run:
            # Time window: last 24 hours
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=24)

            # Try to fetch variables_table unless local_run
            variables_table = {}
            if not args.local_run:
                try:
                    variables_table = metamgr.grab_variables_table(
                        socket_address=args.metamgr_socket_address,
                        socket_port=args.metamgr_socket_port
                    )
                except Exception as e:
                    logger.warning(f"[VALIDATION] Skipping variable-table-based checks: {e}")

            # Variable validations
            variable_validators = [
                validator.validate_vargem_vnums,
                validator.validate_statistic_context_vnum,
                validator.validate_required_variable_fields,
                validator.validate_overlapping_variable_names,
            ]

            if variables_table:
                variable_validators.append(partial(validator.validate_variables, variables_table=variables_table))
                variable_validators[-1].__name__ = "validate_variables"


            # Observation validations
            obs_validators = [
                lambda obs: validator.validate_dattim(obs, start_time, end_time)
            ]
            if variables_table:
                obs_validators.append(partial(validator.validate_observation_ranges, variables_table=variables_table))
                obs_validators[-1].__name__ = "validate_observation_ranges"

            # Run validations
            all_validation_messages = []
            for vfunc in variable_validators:
                for m in vfunc(variables):
                    all_validation_messages.append((vfunc.__name__, m))  # tag with func name
            for ofunc in obs_validators:
                for m in ofunc(grouped_obs):
                    all_validation_messages.append((ofunc.__name__, m))

            if all_validation_messages:
                grouped = defaultdict(list)
                for name, msg in all_validation_messages:
                    grouped[name].append(msg)
                for func_name, msgs in grouped.items():
                    logger.debug(f"[{func_name}] {len(msgs)} occurrences")
                    for m in msgs:
                        logger.debug(f"[{func_name}] {m}")
            else:
                logger.debug(":: PASSED :: All variable and observation validations clean.")

        ########################################################################################################################
        # DIFF AGAINST DATA CACHE AND SEND TO POE
        ########################################################################################################################
        # Load the cache of recent data
        if os.path.exists(seen_obs_file):
            # open seen_obs file, grab last timestamp sent to POE
            with open(seen_obs_file, 'r') as old_row_file:
                old_rows = [i.strip() for i in old_row_file.readlines()]
        else:
            old_rows = []
            
        ###### Submit Data to POE in chunks ######
        for chunk in poe.chunk_list(grouped_obs, chunk_size=int(args.poe_chunk_size)):
            # Process each chunk
            io, seen_obs = poe.poe_formatter(chunk, old_rows)
            # Check if there's data to insert
            if io is None:
                logger.debug("Chunk io is empty — all records already seen")
            elif args.local_run:
                logger.debug("LOCAL_RUN=True — skipping POE insertion")
            else:
                poe.poe_insertion(io, args)
                time.sleep(2)

        # Run poe_formatter one final time on the full set to get the updated
        # seen_obs — does NOT send to POE, only updates the local cache.
        io, seen_obs = poe.poe_formatter(grouped_obs, old_rows)
        logger.debug(f"Final seen_obs count before filtering: {len(seen_obs)}")

        seen_obs = filter_valid_seen_obs(seen_obs)
        seen_obs = poe.seen_obs_formatter(seen_obs, data_archive_time)
        logger.debug(f"seen_obs after pruning: {len(seen_obs)} entries")
            

            ########################################################################################################################
            # UPLOAD TO S3
            ########################################################################################################################
        if not args.local_run:
            # Write an archive file of seen_obs to check for duplicate records in the next run
            with open(seen_obs_file, 'w+') as file:
                for ob in seen_obs:
                    file.write(ob + '\n')
            aws.S3.upload_file(local_file_path=seen_obs_file, bucket=s3_bucket_name,s3_key=s3_seen_obs_file)
            logger.info(f"PERSIST: uploaded seen_obs ({len(seen_obs)} entries)")

        success_flag = 1

    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")

    finally:
        total_runtime = time.time() - start_runtime
        logger.info(msg=json.dumps({'completion': success_flag, 'time': total_runtime}))

        # Overwrite the same S3 object each run in prod
        if not (args.local_run or args.dev) and log_file:
            try:
                s3_log_key = posixpath.join(s3_work_dir, f"{INGEST_NAME}_obs.log")
                aws.S3.upload_file(local_file_path=log_file, bucket=s3_bucket_name, s3_key=s3_log_key)
            except Exception as e:
                logger.warning(f"Failed to upload run log to S3: {e}")

        logging.shutdown()
