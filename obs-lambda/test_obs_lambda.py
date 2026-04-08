"""
test_obs_lambda.py — Local test runner for germanyWSV observation ingest
========================================================================

Usage
-----
    python test_obs_lambda.py

Pre-requisites
--------------
1. Run test_meta_lambda.py first so ../dev/germanyWSV_stations_metadata.json
   exists.  The obs lambda loads this file to resolve UUIDs → STIDs.

2. If you want to run the ingestlib validators against MetaManager, open an
   SSM tunnel first:
       aws ssm start-session ... --document-name ... --parameters \
           '{"portNumber":["8888"],"localPortNumber":["18888"]}'
   Then set DEV=True below (instead of just LOCAL_RUN=True).

Outputs (in ../dev/)
--------------------
    germanyWSV_obs.log     — full log output
    grouped_obs.txt        — one pipe-delimited observation string per line
                             format: WSV0042800310|202505041100|{"water_level_m":{1050:2.393}}
    station_meta.json      — snapshot of loaded station metadata

Notes
-----
* LOCAL_RUN=True → no POE insertion, no S3 reads/writes
* DEV=True + LOCAL_RUN=True → validators run but MetaManager calls are skipped
* Set DEV=True + LOCAL_RUN=False (with SSM tunnel) to run validators against
  the live MetaManager for full pre-prod validation
"""

import os
import logging

# ---------------------------------------------------------------------------
# Environment — must be set BEFORE any import that reads args
# ---------------------------------------------------------------------------
os.environ["AWS_PROFILE"] = "ingest"
os.environ["DEV"] = "True"          # enables validators; set False to skip
os.environ["LOCAL_RUN"] = "True"    # no POE, no S3
os.environ["LOG_LEVEL"] = "DEBUG"

# ---------------------------------------------------------------------------
# SSM Tunnel configuration
# Uncomment and configure these if you want to run validators against
# MetaManager (requires an active SSM tunnel on port 18888).
# ---------------------------------------------------------------------------
# os.environ["POE_SOCKET_ADDRESS"] = "localhost"
# os.environ["POE_SOCKET_PORT"] = "18095"
os.environ["METAMGR_SOCKET_ADDRESS"] = "localhost"
os.environ["METAMGR_SOCKET_PORT"] = "18888"

# Internal bucket — required even in LOCAL_RUN (arg parsing reads it)
os.environ["INTERNAL_BUCKET_NAME"] = "synoptic-ingest-wsv-wsv328"

# ---------------------------------------------------------------------------
# Import AFTER env setup
# ---------------------------------------------------------------------------
from obs_lambda_handler import main
from args import args


# ---------------------------------------------------------------------------
# Dummy Lambda context
# ---------------------------------------------------------------------------
class Context:
    def __init__(self):
        self.function_name = "germanyWSV-obs-local-test"
        self.memory_limit_in_mb = 512
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:000000000000:function:test"
        self.aws_request_id = "local-test-id"


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
print("=" * 80)
print("Starting germanyWSV Observation Ingest (LOCAL TEST)")
print("Fetching live data from: https://www.pegelonline.wsv.de/")
print("Output will be written to: ../dev/")
print("  POE insertion: DISABLED (LOCAL_RUN=True)")
print("  Validators:    ENABLED  (DEV=True)")
print("=" * 80)

response = main({}, Context())

print("=" * 80)
print("Test completed. Check ../dev/ for:")
print("  germanyWSV_obs.log      — execution log")
print("  grouped_obs.txt         — parsed observations (STID|OBTIME|{...})")
print("  station_meta.json       — station metadata snapshot")
print("=" * 80)