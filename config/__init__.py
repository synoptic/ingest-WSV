# config — Germany WSV (Wasserstraßen- und Schifffahrtsverwaltung) ingest configuration
#
# Single source of truth. Read by handlers, CDK stacks, tools, and tests.
#
# Identity fields are used by MetadataIngest / Ingest base classes.
# Infrastructure fields are used by CDK stacks and app.py.
# Connection defaults are baked into Lambda environment dicts by app.py.

# ── Ingest identity ────────────────────────────────────────
NAME            = "germanyWSV"
DISPLAY_NAME    = "Germany WSV"                  # for stack names, descriptions
MNET_ID         = 343
STID_PREFIX     = "WSV"
INCOMING_ELEVATION_UNIT = "METERS"
RESTRICTED_DATA     = False
RESTRICTED_METADATA = False
LOOKUP_KEY = "STID"

# ── Attribution ───────────────────────────────────────────
ATTRIBUTIONS = [
    {
        "owner": {"name": "Germany Wasserstraßen- und Schifffahrtsverwaltung", "ownertype_id": 10},
        "tier": 1,
        "type_id": 10,
    },
]

# ── MNET (for generate-mnet SQL) ──────────────────────────
MNET_SHORTNAME = "WSV"
MNET_LONGNAME  = "Germany Wasserstraßen- und Schifffahrtsverwaltung"
MNET_URL       = "https://www.pegelonline.wsv.de/"
MNET_OBTAIN    = "DIRECT"
MNET_ACQMETHOD = "API"
MNET_CATEGORY  = 14              # 14 = surface synoptic / land-based met network

# ── AWS account & networking ──────────────────────────────
ACCOUNT    = "905418025696"
REGION     = "us-east-1"
VPC_ID     = "vpc-09a07e46ba606169b"
SUBNET_IDS = [
    "subnet-07287ffd7252971f0",
    "subnet-069ceb1375036964b",
]

# ── S3 buckets ────────────────────────────────────────────
INGEST_S3_BUCKET = "synoptic-ingest-wsv-wsv328"
CACHE_S3_BUCKET  = "synoptic-ingest-provider-data-cache-a4fb6"

# ── Connection defaults ───────────────────────────────────
POE_ADDRESS    = "mesonet-v2.entry.int.synopticdata.net"
POE_PORT       = "8095"
POE_CHUNK_SIZE = "2000"
METAMGR_ADDRESS = "10.14.159.245"
METAMGR_PORT    = "8888"

# ── Obs event source ─────────────────────────────────────
# "schedule" — CloudWatch cron, runs every OBS_SCHEDULE_MINUTES
# "queue"    — SQS, triggered by messages on OBS_QUEUE_URL
OBS_EVENT_SOURCE     = "schedule"
OBS_SCHEDULE_MINUTES = 15
MNET_PERIOD = OBS_SCHEDULE_MINUTES

# ── Meta schedule ─────────────────────────────────────────
META_EVENT_SOURCE  = "schedule"
META_SCHEDULE_CRON = {"hour": "0", "minute": "0"}

# ── Lambda sizing ─────────────────────────────────────────
OBS_COMPUTE      = "lambda"                      # "lambda" or "fargate"
OBS_MEMORY_MB    = 512
OBS_TIMEOUT_MIN  = 4
OBS_CONCURRENCY  = 1                             # None = unrestricted

META_COMPUTE     = "lambda"                      # "lambda" or "fargate"
META_MEMORY_MB   = 512
META_TIMEOUT_MIN = 10