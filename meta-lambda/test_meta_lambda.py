"""
test_meta_lambda.py — Local test runner for germanyWSV metadata ingest
======================================================================

Usage
-----
    python test_meta_lambda.py

Outputs
-------
All files are written to ../dev/ :
    germanyWSV_meta.log                   — full log output
    germanyWSV_stations_metadata.json     — parsed station metadata (all fields)
    germanyWSV_station_lookup_payload.json — station_lookup payload for review

Notes
-----
* LOCAL_RUN=True means the script will NOT call station_lookup.load_metamgr()
  and will NOT write to S3.  Safe to run from any workstation.
* Set INTERNAL_BUCKET_NAME to a real bucket only if you want S3 reads in dev.
* After running, inspect ../dev/germanyWSV_stations_metadata.json to verify:
    - DWD exact-match stations have dwd_exact_match=true and the correct dwd_stid
    - gauge_zero_value is populated for most W-reporting stations
    - STIDs follow the "WSV" + 10-digit-number pattern
"""

import os
import json
import time

# ---------------------------------------------------------------------------
# Environment — must be set BEFORE importing anything that reads args
# ---------------------------------------------------------------------------
os.environ["AWS_PROFILE"] = "ingest"
os.environ["DEV"] = "False"         # False = don't try to connect to MetaManager
os.environ["LOCAL_RUN"] = "True"    # True = write to ../dev, skip S3 and station_lookup
os.environ["LOG_LEVEL"] = "DEBUG"
os.environ["FIRST_RUN"] = "True"

# Point at the WSV bucket; ignored in LOCAL_RUN mode but must be set for arg parsing
os.environ["INTERNAL_BUCKET_NAME"] = "synoptic-ingest-wsv-wsv328"

# ---------------------------------------------------------------------------
# Import AFTER env setup
# ---------------------------------------------------------------------------
from meta_lambda_handler import main
from args import args


# ---------------------------------------------------------------------------
# Dummy Lambda context
# ---------------------------------------------------------------------------
class Context:
    def __init__(self):
        self.function_name = "germanyWSV-meta-local-test"
        self.memory_limit_in_mb = 512
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:000000000000:function:test"
        self.aws_request_id = "local-test-id"


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
print("=" * 80)
print("Starting germanyWSV Metadata Ingest (LOCAL TEST)")
print("Fetching all stations from: https://www.pegelonline.wsv.de/")
print("Output will be written to: ../dev/")
print("=" * 80)

event = {}
response = main(event, Context())

print("=" * 80)
print("Test completed. Check ../dev/ for:")
print("  germanyWSV_meta.log")
print("  germanyWSV_stations_metadata.json")
print("  germanyWSV_station_lookup_payload.json")
print("=" * 80)