"""
Germany WSV metadata ingest

Fetches station metadata from the Pegelonline REST API and registers all
stations with station_lookup. Supports local mode.

~600+ river-gauge stations; each gets its own sequential WSV STID.
"""

import json
import os
import requests
import unicodedata

from ingestlib.metadata import MetadataIngest
from ingestlib.core import make_lambda_handler
from config import NAME, MNET_ID, STID_PREFIX, INCOMING_ELEVATION_UNIT

# ── Constants ──────────────────────────────────────────────────────
PEGELONLINE_BASE = "https://www.pegelonline.wsv.de/webservices/rest-api/v2"
STATIONS_URL     = f"{PEGELONLINE_BASE}/stations.json?includeTimeseries=true"


class GermanyWSVMeta(MetadataIngest):
    NAME           = NAME
    MNET_ID        = MNET_ID
    STID_PREFIX    = STID_PREFIX
    ELEVATION_UNIT = INCOMING_ELEVATION_UNIT

    # ── Local mode helpers (HKO pattern) ───────────────────────────
    def _is_local(self) -> bool:
        return os.environ.get("MODE") == "local"

    def setup(self):
        if self._is_local():
            self.logger.info("SETUP: LOCAL mode - Pegelonline public API")
            path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "dev",
                f"{self.NAME}_stations_metadata.json",
            )
            if os.path.exists(path):
                with open(path) as f:
                    self.existing_stations = json.load(f)
                self.logger.info(f"Loaded {len(self.existing_stations)} stations (local)")
            else:
                self.existing_stations = {}
        else:
            super().setup()

    def acquire(self):
        """Fetch all stations from the Pegelonline REST API."""
        self.logger.info(f"ACQUIRE: fetching {STATIONS_URL}")
        resp = requests.get(STATIONS_URL, timeout=60)
        resp.raise_for_status()
        stations = resp.json()
        self.logger.info(f"ACQUIRE: received {len(stations)} raw station records")
        return stations

    def parse(self, raw_stations: list) -> dict:
        """
        Parse Pegelonline stations into station_meta. Every station receives
        its own sequential WSV STID regardless of any overlap with other networks.
        """
        station_meta = dict(self.existing_stations)
        assigned_stids = {
            s.get("SYNOPTIC_STID")
            for s in self.existing_stations.values()
            if s.get("SYNOPTIC_STID")
        }
        next_stid_seq = 1

        counters = {
            "accepted": 0, "skipped_no_uuid": 0, "skipped_bad_coords": 0,
        }

        def _next_sequential_stid() -> str:
            nonlocal next_stid_seq
            while True:
                candidate = f"{self.STID_PREFIX}{next_stid_seq:04d}"
                next_stid_seq += 1
                if candidate not in assigned_stids:
                    return candidate

        for stn in raw_stations:
            uuid      = stn.get("uuid")
            number    = stn.get("number")
            shortname = stn.get("shortname", "")
            longname  = stn.get("longname") or shortname
            longname  = unicodedata.normalize("NFKD", longname).encode("ascii", "ignore").decode("ascii")

            if not uuid:
                counters["skipped_no_uuid"] += 1
                continue

            # Coordinates — skip only if outright missing or (0, 0)
            try:
                lat = float(stn["latitude"])
                lon = float(stn["longitude"])
            except (KeyError, TypeError, ValueError):
                counters["skipped_bad_coords"] += 1
                continue
            if lat == 0.0 and lon == 0.0:
                counters["skipped_bad_coords"] += 1
                continue

            # Elevation (Pegelonline "km" = river-km, not elevation)
            elevation = None

            # Every WSV station gets its own sequential STID
            canonical_stid = _next_sequential_stid()
            assigned_stids.add(canonical_stid)

            station_meta[uuid] = {
                "SYNOPTIC_STID": canonical_stid,
                "NAME":          longname,
                "LATITUDE":      lat,
                "LONGITUDE":     lon,
                "ELEVATION":     elevation,
                "OTHER_ID":      number,        # Source ID (Pegelonline number)
            }
            counters["accepted"] += 1

        self.logger.info(
            f"PARSE: accepted={counters['accepted']}, "
            f"skipped_no_uuid={counters['skipped_no_uuid']}, "
            f"skipped_bad_coords={counters['skipped_bad_coords']}"
        )
        return station_meta

    def save_station_meta(self, station_meta: dict):
        if self._is_local():
            path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "dev",
                f"{self.NAME}_stations_metadata.json",
            )
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                json.dump(station_meta, f, indent=4)
            self.logger.info(f"LOCAL SAVE: wrote {len(station_meta)} stations")
        else:
            super().save_station_meta(station_meta)

# ── Entry points ───────────────────────────────────────────────────
lambda_handler = make_lambda_handler(GermanyWSVMeta)

if __name__ == "__main__":
    import sys
    if "--local" in sys.argv:
        os.environ["MODE"] = "local"
    GermanyWSVMeta().run()