"""
Mine Haulage Telemetry Emitter
==============================
Simulates live CAT-797 truck telemetry by writing haulage trip cycles
to PostgreSQL. Debezium reads the WAL and streams changes to Kafka.

Cycle per truck:
  INSERT (LOADING) → UPDATE (HAULING) → UPDATE (DUMPING) → UPDATE (IDLE)

Usage:
  python telemetry_emitter.py               # default: 1 truck at a time
  python telemetry_emitter.py --trucks 3    # 3 concurrent trucks
  python telemetry_emitter.py --dry-run     # print SQL without hitting DB
"""

import argparse
import logging
import os
import random
import signal
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ─────────────────────────── configuration ────────────────────────────────

load_dotenv()

# Fixed master lists so every run uses the same IDs (reproducible demo)
TRUCKS: list[str] = [f"TRK-CAT-797-{i:02d}" for i in range(1, 6)]
OPERATORS: list[str] = [
    "OP-3821", "OP-4417", "OP-5503", "OP-6672", "OP-7890",
]
MATERIALS: list[str] = ["ORE", "WASTE"]
PIT_LOCATIONS: list[str] = ["LOC-PIT-ALPHA", "LOC-PIT-BRAVO"]
DUMP_LOCATIONS: list[str] = ["LOC-CRUSHER-01", "LOC-STOCKPILE-A"]

# Cycle timing (seconds) — feel free to shorten for demo recordings
LOADING_TIME = (5, 10)   # time spent loading at pit
HAULING_TIME = (8, 15)   # travel time to dump
DUMPING_TIME = (4, 8)    # time spent dumping
IDLE_TIME = (3, 6)    # cool-down before next cycle

# Postgres reconnect policy
MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_BACKOFF_BASE = 2  # seconds, doubles each attempt

# ──────────────────────────── logging setup ───────────────────────────────

LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

log = logging.getLogger("telemetry.emitter")


# ─────────────────────────── data models ──────────────────────────────────

@dataclass
class TripRecord:

    equipment_id: str
    operator_id: str
    source_location_id: str
    destination_location_id: str
    material_type: str
    load_timestamp: datetime = field(default_factory=datetime.now)
    trip_id: Optional[int] = None

    def __str__(self) -> str:
        return (
            f"[{self.equipment_id}] "
            f"{self.material_type} | "
            f"{self.source_location_id} → {self.destination_location_id} | "
            f"op={self.operator_id}"
        )


@dataclass
class EmitterStats:
    """Tracks cumulative stats for the session."""
    total_trips: int = 0
    total_ore_trips: int = 0
    total_waste_trips: int = 0
    errors: int = 0
    session_start: datetime = field(default_factory=datetime.now)

    def record_trip(self, material: str) -> None:
        self.total_trips += 1
        if material == "ORE":
            self.total_ore_trips += 1
        else:
            self.total_waste_trips += 1

    def summary(self) -> str:
        elapsed = datetime.now() - self.session_start
        mins = int(elapsed.total_seconds() // 60)
        secs = int(elapsed.total_seconds() % 60)
        return (
            f"Session {mins}m {secs}s | "
            f"Trips: {self.total_trips} "
            f"(ORE={self.total_ore_trips}, WASTE={self.total_waste_trips}) | "
            f"Errors: {self.errors}"
        )


# ──────────────────────────── database layer ──────────────────────────────

def build_dsn() -> dict:

    return {
        "host":     os.getenv("POSTGRES_HOST", "localhost"),
        "port":     int(os.getenv("POSTGRES_PORT", 5432)),
        "database": os.getenv("POSTGRES_DB", "postgres"),
        "user":     os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def connect_with_retry(dsn: dict) -> psycopg2.extensions.connection:

    for attempt in range(1, MAX_RECONNECT_ATTEMPTS + 1):
        try:
            conn = psycopg2.connect(
                **dsn, cursor_factory=psycopg2.extras.RealDictCursor)
            conn.autocommit = False
            log.info("Connected to PostgreSQL at %s:%s/%s",
                     dsn["host"], dsn["port"], dsn["database"])
            return conn
        except psycopg2.OperationalError as exc:
            wait = RECONNECT_BACKOFF_BASE ** attempt
            log.warning(
                "Connection attempt %d/%d failed: %s — retrying in %ds",
                attempt, MAX_RECONNECT_ATTEMPTS, exc, wait,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Could not connect to PostgreSQL after {MAX_RECONNECT_ATTEMPTS} attempts."
    )


def ensure_connection(
    conn: Optional[psycopg2.extensions.connection],
    dsn: dict,
) -> psycopg2.extensions.connection:
    """Return a healthy connection, reconnecting if needed."""
    if conn is None or conn.closed:
        log.info("No active connection — establishing new one...")
        return connect_with_retry(dsn)
    try:
        conn.cursor().execute("SELECT 1")
        return conn
    except psycopg2.Error:
        log.warning("Stale connection detected — reconnecting...")
        try:
            conn.close()
        except Exception:
            pass
        return connect_with_retry(dsn)


@contextmanager
def transaction(conn: psycopg2.extensions.connection):
    """Context manager: commit on success, rollback on any exception."""
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ─────────────────────────── SQL statements ───────────────────────────────

SQL_INSERT = """
    INSERT INTO haulage_trips (
        equipment_id,
        operator_id,
        source_location_id,
        destination_location_id,
        load_timestamp,
        dump_timestamp,
        material_type,
        current_status
    )
    VALUES (
        %(equipment_id)s,
        %(operator_id)s,
        %(source_location_id)s,
        %(destination_location_id)s,
        %(load_timestamp)s,
        NULL,
        %(material_type)s,
        'LOADING'
    )
    RETURNING trip_id;
"""

SQL_UPDATE_STATUS = """
    UPDATE haulage_trips
    SET    current_status = %(status)s
    WHERE  trip_id = %(trip_id)s;
"""

SQL_UPDATE_DUMPING = """
    UPDATE haulage_trips
    SET    current_status = 'DUMPING',
           dump_timestamp = %(dump_timestamp)s
    WHERE  trip_id = %(trip_id)s;
"""

SQL_UPDATE_IDLE = """
    UPDATE haulage_trips
    SET    current_status = 'IDLE'
    WHERE  trip_id = %(trip_id)s;
"""


# ─────────────────────────── emitter logic ────────────────────────────────

def make_trip(truck_id: str) -> TripRecord:

    return TripRecord(
        equipment_id=truck_id,
        operator_id=random.choice(OPERATORS),
        source_location_id=random.choice(PIT_LOCATIONS),
        destination_location_id=random.choice(DUMP_LOCATIONS),
        material_type=random.choice(MATERIALS),
        load_timestamp=datetime.now(),
    )


def run_trip_cycle(
    trip: TripRecord,
    conn: psycopg2.extensions.connection,
    stats: EmitterStats,
    dry_run: bool = False,
) -> None:

    # ── 1. INSERT (LOADING) ───────────────────────────────────────────────
    log.info("⛏  LOADING  %s", trip)

    if not dry_run:
        with transaction(conn) as c:
            cur = c.cursor()
            cur.execute(SQL_INSERT, {
                "equipment_id":           trip.equipment_id,
                "operator_id":            trip.operator_id,
                "source_location_id":     trip.source_location_id,
                "destination_location_id": trip.destination_location_id,
                "load_timestamp":         trip.load_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "material_type":          trip.material_type,
            })
            trip.trip_id = cur.fetchone()["trip_id"]
        log.info("   → Inserted trip_id=%d", trip.trip_id)

    _sleep(*LOADING_TIME)

    # ── 2. UPDATE (HAULING) ───────────────────────────────────────────────
    log.info("🚚  HAULING  %s  trip_id=%s", trip.equipment_id, trip.trip_id)

    if not dry_run:
        with transaction(conn) as c:
            c.cursor().execute(SQL_UPDATE_STATUS, {
                "status":  "HAULING",
                "trip_id": trip.trip_id,
            })

    _sleep(*HAULING_TIME)

    # ── 3. UPDATE (DUMPING) ───────────────────────────────────────────────
    dump_time = datetime.now()
    log.info("🪨  DUMPING  %s  trip_id=%s", trip.equipment_id, trip.trip_id)

    if not dry_run:
        with transaction(conn) as c:
            c.cursor().execute(SQL_UPDATE_DUMPING, {
                "dump_timestamp": dump_time.strftime("%Y-%m-%d %H:%M:%S"),
                "trip_id":        trip.trip_id,
            })

    _sleep(*DUMPING_TIME)

    # ── 4. UPDATE (IDLE) ──────────────────────────────────────────────────
    log.info("💤  IDLE     %s  trip_id=%s  cycle complete",
             trip.equipment_id, trip.trip_id)

    if not dry_run:
        with transaction(conn) as c:
            c.cursor().execute(SQL_UPDATE_IDLE, {"trip_id": trip.trip_id})

    stats.record_trip(trip.material_type)
    log.info("   %s", stats.summary())

    _sleep(*IDLE_TIME)


def _sleep(lo: int, hi: int) -> None:

    duration = random.randint(lo, hi)
    log.debug("   sleeping %ds...", duration)
    time.sleep(duration)


# ──────────────────────────── main entry ──────────────────────────────────

_shutdown_requested = False


def _handle_signal(signum, _frame):
    global _shutdown_requested
    log.info("Signal %s received — finishing current cycle then stopping...", signum)
    _shutdown_requested = True


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mine haulage telemetry emitter")
    p.add_argument(
        "--trucks",
        type=int,
        default=1,
        choices=range(1, len(TRUCKS) + 1),
        metavar=f"1-{len(TRUCKS)}",
        help="Number of trucks to cycle through per loop (default: 1)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print log output without writing to the database",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible demo runs",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        log.info("Random seed set to %d", args.seed)

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    dsn = build_dsn()
    conn: Optional[psycopg2.extensions.connection] = None
    stats = EmitterStats()

    # Select the active truck pool for this run
    active_trucks = TRUCKS[:args.trucks]
    log.info("Starting emitter — trucks=%s  dry_run=%s",
             active_trucks, args.dry_run)

    if args.dry_run:
        log.warning("DRY-RUN mode: no database writes will occur")
    else:
        conn = connect_with_retry(dsn)

    try:
        while not _shutdown_requested:
            for truck_id in active_trucks:
                if _shutdown_requested:
                    break

                # Guarantee a live connection before each trip
                if not args.dry_run:
                    conn = ensure_connection(conn, dsn)

                trip = make_trip(truck_id)

                try:
                    run_trip_cycle(trip, conn, stats, dry_run=args.dry_run)
                except psycopg2.Error as exc:
                    stats.errors += 1
                    log.error(
                        "DB error on trip for %s (trip_id=%s): %s",
                        truck_id, trip.trip_id, exc,
                    )
                    # Force reconnect on next cycle
                    if conn and not conn.closed:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    conn = None
                except Exception as exc:
                    stats.errors += 1
                    log.error("Unexpected error during trip cycle: %s",
                              exc, exc_info=True)

    finally:
        log.info("Shutting down. Final stats: %s", stats.summary())
        if conn and not conn.closed:
            conn.close()
            log.info("Database connection closed.")


if __name__ == "__main__":
    main()
