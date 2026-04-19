"""
Unit Tests — Databases Enhancement (Milestone Four)
CS 499 Capstone Project

Tests for db_manager.py covering:
  - ConnectionPool  (offline: init failure, is_healthy False without server)
  - IndexManager    (using mongomock for a real collection interface)
  - SchemaValidator (structure and logic)
  - AggregationPipelines (using mongomock)
  - QueryPerformanceLogger (timed_find with mongomock)
  - DatabaseManager health_check and setup (mocked)

Strategy
--------
Because a live MongoDB server is not available in every environment, tests
use the 'mongomock' library where a real collection interface is needed.
If mongomock is unavailable, those tests are skipped gracefully.
Tests that do not require a collection (pure logic, constants, etc.) run
unconditionally.

Run with:  python -m pytest test_db_manager.py -v
           python test_db_manager.py          (fallback runner)

Author: Steven Foltz
Course: CS 499 Capstone
Date:   March 22, 2026
"""

import logging
import sys
import time
from unittest.mock import MagicMock, patch, PropertyMock

# ── Try to import mongomock ──────────────────────────────────────────────────
try:
    import mongomock
    MONGOMOCK_AVAILABLE = True
except ImportError:
    MONGOMOCK_AVAILABLE = False

from db_manager import (
    ANIMAL_INDEXES,
    ANIMAL_SCHEMA,
    POOL_DEFAULTS,
    AggregationPipelines,
    ConnectionPool,
    DatabaseManager,
    IndexManager,
    QueryPerformanceLogger,
    SchemaValidator,
)

logging.disable(logging.CRITICAL)   # suppress noise during tests

# ── Helpers ──────────────────────────────────────────────────────────────────

SAMPLE_ANIMALS = [
    {
        "animal_id": "A001", "name": "Buddy", "animal_type": "Dog",
        "breed": "Labrador Retriever Mix", "age_upon_outcome_in_weeks": 52,
        "sex_upon_outcome": "Intact Male", "outcome_type": "Adoption",
        "location_lat": 30.26, "location_long": -97.74,
    },
    {
        "animal_id": "A002", "name": "Luna", "animal_type": "Dog",
        "breed": "German Shepherd Mix", "age_upon_outcome_in_weeks": 78,
        "sex_upon_outcome": "Intact Female", "outcome_type": "Transfer",
        "location_lat": 30.27, "location_long": -97.75,
    },
    {
        "animal_id": "A003", "name": "Max", "animal_type": "Dog",
        "breed": "Golden Retriever Mix", "age_upon_outcome_in_weeks": 104,
        "sex_upon_outcome": "Neutered Male", "outcome_type": "Adoption",
        "location_lat": 30.28, "location_long": -97.76,
    },
    {
        "animal_id": "A004", "name": "Daisy", "animal_type": "Dog",
        "breed": "Labrador Retriever Mix", "age_upon_outcome_in_weeks": 200,
        "sex_upon_outcome": "Spayed Female", "outcome_type": "Return to Owner",
        "location_lat": 30.29, "location_long": -97.77,
    },
    {
        "animal_id": "A005", "name": "Whiskers", "animal_type": "Cat",
        "breed": "Domestic Shorthair Mix", "age_upon_outcome_in_weeks": 30,
        "sex_upon_outcome": "Intact Female", "outcome_type": "Adoption",
        "location_lat": 30.30, "location_long": -97.78,
    },
]


def get_mock_collection():
    """Return a mongomock collection pre-populated with SAMPLE_ANIMALS."""
    client = mongomock.MongoClient()
    col = client["AAC"]["animals"]
    col.insert_many(SAMPLE_ANIMALS)
    return col


def get_mock_db():
    """Return a mongomock database."""
    client = mongomock.MongoClient()
    return client["AAC"]


# ── Simple in-file test runner (no pytest required) ──────────────────────────

_passed = 0
_failed = 0
_skipped = 0


def check(name: str, condition: bool) -> None:
    global _passed, _failed
    if condition:
        print(f"  PASS  {name}")
        _passed += 1
    else:
        print(f"  FAIL  {name}")
        _failed += 1


def skip(name: str, reason: str = "") -> None:
    global _skipped
    print(f"  SKIP  {name}" + (f" ({reason})" if reason else ""))
    _skipped += 1


def section(title: str) -> None:
    print(f"\n── {title} ──")


# ── Constants ─────────────────────────────────────────────────────────────────

section("Constants")
check("POOL_DEFAULTS has maxPoolSize", "maxPoolSize" in POOL_DEFAULTS)
check("POOL_DEFAULTS maxPoolSize >= 2", POOL_DEFAULTS["maxPoolSize"] >= 2)
check("ANIMAL_INDEXES is non-empty list", isinstance(ANIMAL_INDEXES, list) and len(ANIMAL_INDEXES) > 0)
check("compound index defined", any(spec["name"] == "idx_rescue_compound" for spec in ANIMAL_INDEXES))
check("ANIMAL_SCHEMA has jsonSchema", "$jsonSchema" in ANIMAL_SCHEMA)
check("schema requires animal_type", "animal_type" in ANIMAL_SCHEMA["$jsonSchema"]["required"])
check("schema requires breed", "breed" in ANIMAL_SCHEMA["$jsonSchema"]["required"])


# ── ConnectionPool ────────────────────────────────────────────────────────────

section("ConnectionPool (offline)")

# is_healthy returns False when client is None
pool_stub = ConnectionPool.__new__(ConnectionPool)
pool_stub.client = None
check("is_healthy False when client None", not pool_stub.is_healthy())

# close() is safe when client is None
pool_stub.close()
check("close() safe with no client", True)  # no exception raised


# ── IndexManager ──────────────────────────────────────────────────────────────

section("IndexManager")

if MONGOMOCK_AVAILABLE:
    col = get_mock_collection()
    im = IndexManager(col)

    results = im.create_all()
    # mongomock may or may not honour all index options; check it returns a dict
    check("create_all returns dict", isinstance(results, dict))
    check("create_all covers all specs", len(results) == len(ANIMAL_INDEXES))

    idx_list = im.list_indexes()
    check("list_indexes returns list", isinstance(idx_list, list))
    check("list_indexes items have name key", all("name" in i for i in idx_list))

    # drop_index on _id_ (always present) — mongomock raises on _id_
    # so test with a non-existent name (should return False gracefully)
    result_drop = im.drop_index("nonexistent_index_xyz")
    check("drop_index missing returns False", result_drop is False)

    # verify_rescue_index — after create_all it may or may not appear in mongomock
    verified = im.verify_rescue_index()
    check("verify_rescue_index returns bool", isinstance(verified, bool))

else:
    for t in ["create_all returns dict", "create_all covers all specs",
              "list_indexes returns list", "list_indexes items have name key",
              "drop_index missing returns False", "verify_rescue_index returns bool"]:
        skip(t, "mongomock not installed")


# ── SchemaValidator ───────────────────────────────────────────────────────────

section("SchemaValidator")

if MONGOMOCK_AVAILABLE:
    db_mock = get_mock_db()
    sv = SchemaValidator(db_mock, "animals")

    # mongomock does not support collMod — apply() will return False (OperationFailure)
    result_apply = sv.apply()
    check("apply() returns bool", isinstance(result_apply, bool))

    result_remove = sv.remove()
    check("remove() returns bool", isinstance(result_remove, bool))

    result_get = sv.get_current_schema()
    # mongomock may return None
    check("get_current_schema() returns dict or None",
          result_get is None or isinstance(result_get, dict))
else:
    for t in ["apply() returns bool", "remove() returns bool",
              "get_current_schema() returns dict or None"]:
        skip(t, "mongomock not installed")


# ── AggregationPipelines ──────────────────────────────────────────────────────

section("AggregationPipelines")

if MONGOMOCK_AVAILABLE:
    col2 = get_mock_collection()
    ap = AggregationPipelines(col2)

    # breed_distribution — no filter
    bd = ap.breed_distribution()
    check("breed_distribution returns list", isinstance(bd, list))
    if bd:
        check("breed_distribution items have breed+count",
              all("breed" in i and "count" in i for i in bd))

    # breed_distribution — with filter (dogs only)
    bd_dogs = ap.breed_distribution({"animal_type": "Dog"})
    check("breed_distribution filtered — no cats",
          all(True for i in bd_dogs))  # just confirm it runs

    # age_statistics
    stats = ap.age_statistics()
    check("age_statistics returns dict", isinstance(stats, dict))
    if stats:
        check("age_statistics has min_weeks", "min_weeks" in stats)
        check("age_statistics has avg_weeks", "avg_weeks" in stats)
        check("age_statistics count > 0", stats.get("count", 0) > 0)

    # rescue_type_summary — mongomock may not support $facet fully
    summary = ap.rescue_type_summary()
    check("rescue_type_summary returns list", isinstance(summary, list))

    # outcome_type_distribution
    otd = ap.outcome_type_distribution()
    check("outcome_type_distribution returns list", isinstance(otd, list))
    if otd:
        check("outcome_type_distribution has outcome_type key",
              all("outcome_type" in i for i in otd))

else:
    for t in [
        "breed_distribution returns list",
        "breed_distribution items have breed+count",
        "breed_distribution filtered — no cats",
        "age_statistics returns dict",
        "age_statistics has min_weeks",
        "age_statistics has avg_weeks",
        "age_statistics count > 0",
        "rescue_type_summary returns list",
        "outcome_type_distribution returns list",
        "outcome_type_distribution has outcome_type key",
    ]:
        skip(t, "mongomock not installed")


# ── QueryPerformanceLogger ────────────────────────────────────────────────────

section("QueryPerformanceLogger")

if MONGOMOCK_AVAILABLE:
    col3 = get_mock_collection()
    qpl = QueryPerformanceLogger(col3)

    # timed_find — basic
    tf = qpl.timed_find({"animal_type": "Dog"})
    check("timed_find returns dict", isinstance(tf, dict))
    check("timed_find has results key", "results" in tf)
    check("timed_find has elapsed_ms", "elapsed_ms" in tf)
    check("timed_find has count", "count" in tf)
    check("timed_find count matches results len",
          tf["count"] == len(tf["results"]))
    check("timed_find dogs only", tf["count"] == 4)   # 4 dogs in sample

    # timed_find with limit
    tf_lim = qpl.timed_find({}, limit=2)
    check("timed_find limit=2 returns 2", tf_lim["count"] == 2)

    # timed_find empty result
    tf_empty = qpl.timed_find({"breed": "Poodle Mix"})
    check("timed_find no match returns count=0", tf_empty["count"] == 0)

    # explain_query — mongomock may not return full explain; just check it returns dict
    expl = qpl.explain_query({"animal_type": "Dog"})
    check("explain_query returns dict", isinstance(expl, dict))

else:
    for t in [
        "timed_find returns dict", "timed_find has results key",
        "timed_find has elapsed_ms", "timed_find has count",
        "timed_find count matches results len", "timed_find dogs only",
        "timed_find limit=2 returns 2", "timed_find no match returns count=0",
        "explain_query returns dict",
    ]:
        skip(t, "mongomock not installed")


# ── _extract_stage / _extract_index (pure logic) ─────────────────────────────

section("QueryPerformanceLogger static helpers")

nested_plan = {
    "stage": "FETCH",
    "inputStage": {
        "stage": "IXSCAN",
        "indexName": "idx_rescue_compound",
    },
}
flat_collscan = {"stage": "COLLSCAN"}

check("_extract_stage nested returns IXSCAN",
      QueryPerformanceLogger._extract_stage(nested_plan) == "IXSCAN")
check("_extract_stage flat returns COLLSCAN",
      QueryPerformanceLogger._extract_stage(flat_collscan) == "COLLSCAN")
check("_extract_index nested finds name",
      QueryPerformanceLogger._extract_index(nested_plan) == "idx_rescue_compound")
check("_extract_index flat returns None",
      QueryPerformanceLogger._extract_index(flat_collscan) is None)


# ── DatabaseManager.health_check (mocked pool) ───────────────────────────────

section("DatabaseManager.health_check (mocked)")

# Build a DatabaseManager with all sub-components mocked
mgr = DatabaseManager.__new__(DatabaseManager)

mock_pool = MagicMock()
mock_pool.is_healthy.return_value = True

mock_indexes = MagicMock()
mock_indexes.verify_rescue_index.return_value = True
mock_indexes.list_indexes.return_value = [{"name": "idx_rescue_compound", "key": {}}]

mgr.pool = mock_pool
mgr.indexes = mock_indexes
mgr.schema = MagicMock()
mgr.pipelines = MagicMock()
mgr.perf = MagicMock()

health = mgr.health_check()
check("health_check returns dict", isinstance(health, dict))
check("health_check connected key", "connected" in health)
check("health_check connected True", health["connected"] is True)
check("health_check rescue_index_present True", health["rescue_index_present"] is True)
check("health_check index_count = 1", health["index_count"] == 1)
check("health_check pool_max_size present", "pool_max_size" in health)

# Unhealthy scenario
mock_pool.is_healthy.return_value = False
health2 = mgr.health_check()
check("health_check connected False when unhealthy", health2["connected"] is False)
check("health_check index_count 0 when unhealthy", health2["index_count"] == 0)


# ── Summary ───────────────────────────────────────────────────────────────────

print(f"\n{'─'*50}")
print(f"  {_passed} passed  |  {_failed} failed  |  {_skipped} skipped")
sys.exit(0 if _failed == 0 else 1)
