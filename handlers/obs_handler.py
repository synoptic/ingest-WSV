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

from ingestlib.ingest import Ingest
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
        """
        Transform raw Pegelonline data into grouped_obs_set.

        For each station → timeseries → currentMeasurement, resolves the
        SYNOPTIC_STID from station_meta. Stations not present in metadata
        are skipped — the metadata lambda must run first to register them.

        All registered stations are processed — DWD-matched stations are
        not filtered.
        """
        # Build fast uuid → SYNOPTIC_STID lookup from metadata
        uuid_to_stid: dict[str, str] = {
            uuid: meta["SYNOPTIC_STID"]
            for uuid, meta in self.station_meta.items()
            if meta.get("SYNOPTIC_STID")
        }

        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=self.HOURS_TO_RETAIN)
        counters  = defaultdict(int)

        grouped_obs_set: dict[str, dict] = {}

        for stn in raw_stations:
            uuid      = stn.get("uuid")
            shortname = stn.get("shortname", "UNKNOWN")

            # ── Resolve STID ───────────────────────────────────────
            # If the station is not in metadata, skip it. The metadata
            # lambda must run first to register the station.
            if uuid not in uuid_to_stid:
                counters["skipped_not_in_meta"] += 1
                self.logger.debug(
                    f"PARSE: uuid={uuid} ({shortname}) not in station_meta; skipping"
                )
                continue

            stid = uuid_to_stid[uuid]

            # ── Iterate timeseries ─────────────────────────────────
            for ts in stn.get("timeseries", []):
                ts_short = ts.get("shortname", "")

                if ts_short not in self.variables:
                    continue

                current = ts.get("currentMeasurement")
                if not current:
                    continue

                raw_ts  = current.get("timestamp")
                raw_val = current.get("value")

                if raw_ts is None or raw_val is None:
                    counters["missing_value"] += 1
                    continue

                # datetime
                try:
                    dt_local = datetime.fromisoformat(raw_ts)
                    dt_utc   = dt_local.astimezone(timezone.utc)
                except Exception as exc:
                    self.logger.debug(
                        f"PARSE: bad timestamp for {shortname}/{ts_short}: "
                        f"{raw_ts!r} — {exc}"
                    )
                    counters["bad_datetime"] += 1
                    continue

                if dt_utc < cutoff_dt:
                    counters["old_timestamp"] += 1
                    continue

                # value
                try:
                    value = float(raw_val)
                except (TypeError, ValueError):
                    self.logger.debug(
                        f"PARSE: non-numeric value for {shortname}/{ts_short}: {raw_val!r}"
                    )
                    counters["bad_value"] += 1
                    continue

                var_def       = self.variables[ts_short]
                vargem        = var_def["vargem"]
                vnum          = int(var_def["VNUM"])
                final_value = round(value, 3)

                # insert
                obs_time_str = dt_utc.strftime("%Y%m%d%H%M")
                key = f"{stid}|{obs_time_str}"
                grouped_obs_set.setdefault(key, {}).setdefault(vargem, {})[vnum] = final_value
                counters["accepted"] += 1

        self.logger.info(
            f"PARSE summary — "
            f"accepted={counters['accepted']}, "
            f"skipped_not_in_meta={counters['skipped_not_in_meta']}, "
            f"old_timestamp={counters['old_timestamp']}, "
            f"missing_value={counters['missing_value']}, "
            f"bad_datetime={counters['bad_datetime']}, "
            f"bad_value={counters['bad_value']}"
        )
        self.logger.debug(f"PARSE: {len(grouped_obs_set)} grouped observation records")

        return grouped_obs_set


# ── Entry points ───────────────────────────────────────────────────

lambda_handler = make_lambda_handler(GermanyWSVIngest)

if __name__ == "__main__":
    GermanyWSVIngest().run()