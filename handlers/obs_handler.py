"""
Germany WSV observation ingest

Fetches real-time river-gauge measurements from the Pegelonline REST API,
parses and unit-converts each variable, and submits to POE.

All stations are included — DWD-matched stations are not filtered.
Stations absent from metadata fall back to a number-derived STID so
observations are never silently dropped.

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
import json
import os

from ingestlib.ingest import Ingest
from ingestlib.core import make_lambda_handler
from config import NAME
from config.variables import variables
from ingestlib import parse


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

    def __init__(self, *args, **kwargs):
        if self._is_local():
            import os
            os.environ['INTERNAL_BUCKET_NAME'] = ''
        super().__init__(*args, **kwargs)
        if self._is_local():
            self.load_station_meta()
            self.load_seen_obs()

    def _is_local(self) -> bool:
        """Check if running in local mode via args or environment variable."""
        # Try to check args.local_run first (set by MODE=local)
        # Import lazily to ensure environment variables are already set
        try:
            from args import Args
            args_instance = Args()
            if hasattr(args_instance, 'local_run'):
                return args_instance.local_run
        except (ImportError, Exception):
            pass
        
        # Fallback to environment variables (in order of priority)
        # MODE=local (from args configuration)
        if os.environ.get("MODE") == "local":
            return True
        
        return False

    def setup(self):
        """Pegelonline is a public API — no auth required."""
        self.variables = variables
        self.logger.info("SETUP: Pegelonline is public, no auth required")

    def load_seen_obs(self):
        """Load seen_obs from S3 or local."""
        if self._is_local():
            seen_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dev', 'seen_obs.txt')
            self.seen_obs = {}
            if os.path.exists(seen_path):
                with open(seen_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                # Parse STID|dattim|json_data format
                                parts = line.split('|', 2)
                                if len(parts) == 3:
                                    key = f"{parts[0]}|{parts[1]}"
                                    data = parts[2]
                                    self.seen_obs[key] = data
                                else:
                                    # Fallback for old format (just key)
                                    self.seen_obs[line] = '{}'
                            except Exception as e:
                                self.logger.warning(f"Failed to parse seen_obs line: {line} - {e}")
                self.logger.info(f"LOCAL STATE: Loaded {len(self.seen_obs)} seen observations from {seen_path}")
            else:
                self.logger.info("LOCAL STATE: No seen_obs.txt found, starting with empty dict")
        else:
            super().load_seen_obs()

    def save_seen_obs(self):
        """Save seen_obs to local file in local mode."""
        if self._is_local():
            seen_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dev', 'seen_obs.txt')
            with open(seen_path, 'w') as f:
                for obs_key, obs_data in sorted(self.seen_obs.items()):
                    f.write(f"{obs_key}|{obs_data}\n")
            self.logger.info(f"LOCAL STATE: Saved {len(self.seen_obs)} seen observations to {seen_path}")
        else:
            super().save_seen_obs()

    def update_seen_obs(self, grouped_obs_set):
        """Update seen_obs with processed observations."""
        if self._is_local():
            for obs_key, obs_data in grouped_obs_set.items():
                # Convert the data dict to JSON string
                json_str = json.dumps(obs_data, sort_keys=True)
                self.seen_obs[obs_key] = json_str
            self.logger.debug(f"LOCAL STATE: Updated seen_obs with {len(grouped_obs_set)} observations")
            # Save immediately in local mode
            self.save_seen_obs()

    def load_station_meta(self):
        """Load station_meta from S3 or local."""
        self.logger.debug(f"[META] load_station_meta called, _is_local={self._is_local()}")
        if self._is_local():
            meta_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dev', 'station_meta.json')
            self.logger.debug(f"[META] Loading local metadata from {meta_path}")
            if os.path.exists(meta_path):
                try:
                    with open(meta_path, 'r') as f:
                        self.station_meta = json.load(f)
                    self.logger.info(f"[META] ✓ Loaded station metadata from {meta_path} with {len(self.station_meta)} stations")
                except Exception as e:
                    self.logger.error(f"[META] ✗ Failed to load metadata: {e}")
                    self.station_meta = {}
            else:
                self.logger.warning(f"[META] File not found: {meta_path}")
                self.station_meta = {}
        else:
            self.logger.debug(f"[META] Production mode - calling super().load_station_meta()")
            super().load_station_meta()

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
        SYNOPTIC_STID from station_meta (falling back to a number-derived
        STID for stations not yet provisioned by the metadata lambda),
        converts units via the variables data dictionary, and merges into
        a grouped dict keyed by "STID|dattim".

        All stations are processed — DWD-matched stations are not filtered.
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
            # Prefer the provisioned SYNOPTIC_STID from metadata.
            # Fall back to a number-derived STID for stations not yet
            # registered by the metadata lambda — observations are never
            # dropped solely because metadata is missing.
            if uuid in uuid_to_stid:
                stid = uuid_to_stid[uuid]
            else:
                number = stn.get("number", "")
                try:
                    stid = f"WSV{str(int(number)).zfill(10)}"
                except (TypeError, ValueError):
                    stid = f"WSV{str(number).replace(' ', '').zfill(10)}"
                counters["fallback_stid"] += 1
                self.logger.debug(
                    f"PARSE: uuid={uuid} ({shortname}) not in station_meta; "
                    f"using fallback STID {stid}"
                )

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

                # unit conversion
                var_def       = self.variables[ts_short]
                vargem        = var_def["vargem"]
                vnum          = int(var_def["VNUM"])
                incoming_unit = parse.get_translated_value(
                    ts_short, variables=self.variables, field="incoming_unit"
                )
                final_unit = var_def["final_unit"]

                if incoming_unit and incoming_unit != final_unit:
                    try:
                        conversion_name = parse.create_conversion(
                            incoming_unit, self.variables, ts_short
                        )
                        final_value = round(
                            parse.convert_units(conversion_name, value), 3
                        )
                    except Exception:
                        final_value = round(value, 3)
                else:
                    final_value = round(value, 3)

                # insert
                obs_time_str = dt_utc.strftime("%Y%m%d%H%M")
                key = f"{stid}|{obs_time_str}"
                grouped_obs_set.setdefault(key, {}).setdefault(vargem, {})[vnum] = final_value
                counters["accepted"] += 1

        self.logger.info(
            f"PARSE summary — "
            f"accepted={counters['accepted']}, "
            f"fallback_stid={counters['fallback_stid']}, "
            f"old_timestamp={counters['old_timestamp']}, "
            f"missing_value={counters['missing_value']}, "
            f"bad_datetime={counters['bad_datetime']}, "
            f"bad_value={counters['bad_value']}"
        )
        self.logger.debug(f"PARSE: {len(grouped_obs_set)} grouped observation records")
        
        # Update seen_obs with all processed observations in local mode
        if self._is_local():
            self.update_seen_obs(grouped_obs_set)
        
        return grouped_obs_set


# ── Entry points ───────────────────────────────────────────────────

lambda_handler = make_lambda_handler(GermanyWSVIngest)

if __name__ == "__main__":
    GermanyWSVIngest().run()