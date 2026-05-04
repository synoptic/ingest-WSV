"""
Germany WSV observation ingest

Fetches real-time river-gauge measurements from the Pegelonline REST API,
parses and unit-converts each variable, and submits to POE.

All stations are included — DWD-matched stations are not filtered.
Stations absent from metadata are skipped; the metadata lambda must run
first to register new stations before their observations are ingested.

Dev workflow
------------
1. Fill in data_dictionary/variables.py with the WSV variable mappings.
   Run with --mode dev.  Check the raw data in ../dev/ cache.

2. Check ../dev/grouped_obs.txt.  The validator output in the log will
   show you what's off.

3. Once grouped_obs looks right, run with --mode prod.
"""

import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from ingestlib.ingest import Ingest, Observation
from ingestlib.core import make_lambda_handler
from config import NAME
from config.variables import variables


# ── Constants ──────────────────────────────────────────────────────

PEGELONLINE_STATIONS_URL = (
    "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    "?includeTimeseries=true"
    "&includeCurrentMeasurement=true"
)

# ── The ingest ─────────────────────────────────────────────────────

class GermanyWSVIngest(Ingest):
    NAME = NAME

    HOURS_TO_RETAIN = 12
    CACHE_RAW_DATA  = True

    def setup(self):
        """Pegelonline is a public API — no auth required."""
        self.variables = variables
        self.logger.info("SETUP: Pegelonline is public, no auth required")

    def acquire(self) -> list:
        """Fetch all current measurements in a single Pegelonline call."""
        self.logger.info(f"ACQUIRE: GET {PEGELONLINE_STATIONS_URL}")
        resp = requests.get(PEGELONLINE_STATIONS_URL, timeout=60)
        resp.raise_for_status()
        stations = resp.json()
        self.logger.info(f"ACQUIRE: received {len(stations)} station records")
        return stations

    def parse(self, raw_stations: list) -> dict:
        uuid_to_stid = {
            uuid: meta["SYNOPTIC_STID"]
            for uuid, meta in self.station_meta.items()
            if meta.get("SYNOPTIC_STID")
        }

        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=self.HOURS_TO_RETAIN)
        counters = defaultdict(int)
        observations = []

        for stn in raw_stations:
            uuid = stn.get("uuid")
            shortname = stn.get("shortname", "UNKNOWN")

            if uuid not in uuid_to_stid:
                counters["skipped_not_in_meta"] += 1
                continue
            stid = uuid_to_stid[uuid]

            for ts in stn.get("timeseries", []):
                ts_short = ts.get("shortname", "")
                if ts_short not in self.variables:
                    continue

                current = ts.get("currentMeasurement") or {}
                raw_ts, raw_val = current.get("timestamp"), current.get("value")
                if raw_ts is None or raw_val is None:
                    counters["missing_value"] += 1
                    continue

                try:
                    dt_utc = datetime.fromisoformat(raw_ts).astimezone(timezone.utc)
                except Exception:
                    counters["bad_datetime"] += 1
                    continue

                if dt_utc < cutoff_dt:
                    counters["old_timestamp"] += 1
                    continue

                observations.append(Observation(
                    stid=stid,
                    dattim=dt_utc,
                    incoming_var=ts_short,
                    raw_value=raw_val,
                ))
                counters["accepted"] += 1

        self.logger.info(f"PARSE summary — {dict(counters)}")
        return observations

# ── Entry points ───────────────────────────────────────────────────

lambda_handler = make_lambda_handler(GermanyWSVIngest)

if __name__ == "__main__":
    GermanyWSVIngest().run()