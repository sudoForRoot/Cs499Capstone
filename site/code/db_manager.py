"""
Database Manager Module
CS 499 Capstone Project - Databases Enhancement (Milestone Four)

This module provides advanced database management capabilities for the
Grazioso Salvare Dashboard, including:

  1. Index Management  — creates and verifies MongoDB indexes that enforce
                         fast, consistent query performance on the fields
                         most frequently used in rescue filtering.

  2. Aggregation Pipelines — pre-built, parameterised MongoDB aggregation
                              pipelines that replace Python-side data
                              processing with server-side computation,
                              reducing network transfer and improving
                              throughput.

  3. Schema Validation  — enforces a JSON Schema on the 'animals' collection
                          so that every document written to the database
                          satisfies field-type and value constraints before
                          being persisted.

  4. Connection Pooling — wraps MongoClient with explicit pool settings and
                          a health-check / reconnect strategy so that the
                          dashboard degrades gracefully under load.

  5. Query Performance Logging — measures and logs explain-plan statistics
                                  for each rescue query, surfacing slow
                                  queries during development and testing.

Author: Steven Foltz
Course: CS 499 Capstone
Date:   March 22, 2026
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (
    CollectionInvalid,
    ConnectionFailure,
    OperationFailure,
    PyMongoError,
    ServerSelectionTimeoutError,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Default connection-pool settings.
POOL_DEFAULTS: Dict[str, int] = {
    "maxPoolSize": 10,        # maximum concurrent connections
    "minPoolSize": 2,         # connections kept alive when idle
    "serverSelectionTimeoutMS": 5_000,
    "connectTimeoutMS": 5_000,
    "socketTimeoutMS": 30_000,
    "waitQueueTimeoutMS": 10_000,
}

#: Indexes to create on the 'animals' collection.
#: Each entry is (key_list, unique, name, sparse).
ANIMAL_INDEXES: List[Dict[str, Any]] = [
    # Single-field indexes for the three most-queried scalar fields
    {
        "keys": [("animal_type", ASCENDING)],
        "unique": False,
        "name": "idx_animal_type",
        "sparse": False,
    },
    {
        "keys": [("age_upon_outcome_in_weeks", ASCENDING)],
        "unique": False,
        "name": "idx_age_weeks",
        "sparse": True,   # some documents may lack this field
    },
    # Compound index optimised for the rescue-type queries:
    #   { animal_type: "Dog", breed: <list>, age: { $gte, $lte } }
    {
        "keys": [
            ("animal_type", ASCENDING),
            ("breed", ASCENDING),
            ("age_upon_outcome_in_weeks", ASCENDING),
        ],
        "unique": False,
        "name": "idx_rescue_compound",
        "sparse": False,
    },
    # Text index for free-text name search
    {
        "keys": [("name", "text")],
        "unique": False,
        "name": "idx_name_text",
        "sparse": True,
    },
]

#: JSON Schema used for collection-level document validation.
ANIMAL_SCHEMA: Dict[str, Any] = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["animal_type", "breed"],
        "properties": {
            "animal_type": {
                "bsonType": "string",
                "description": "Must be a string and is required.",
                "enum": ["Dog", "Cat", "Bird", "Rabbit", "Other"],
            },
            "breed": {
                "bsonType": "string",
                "description": "Must be a non-empty string and is required.",
                "minLength": 1,
            },
            "age_upon_outcome_in_weeks": {
                "bsonType": ["int", "double", "null"],
                "minimum": 0,
                "maximum": 2_000,
                "description": "Age in weeks; must be between 0 and 2000 if present.",
            },
            "location_lat": {
                "bsonType": ["double", "int", "null"],
                "minimum": -90,
                "maximum": 90,
            },
            "location_long": {
                "bsonType": ["double", "int", "null"],
                "minimum": -180,
                "maximum": 180,
            },
            "sex_upon_outcome": {
                "bsonType": ["string", "null"],
            },
            "outcome_type": {
                "bsonType": ["string", "null"],
            },
        },
        "additionalProperties": True,   # allow extra fields in existing data
    }
}


# ---------------------------------------------------------------------------
# 1. Connection Pool Manager
# ---------------------------------------------------------------------------

class ConnectionPool:
    """
    Manages a single MongoClient instance with connection-pool settings and
    automatic health checking.

    Using a shared pool means that all application components reuse
    established TCP connections rather than opening a new socket for every
    request, reducing latency and preventing resource exhaustion.

    Attributes
    ----------
    client : MongoClient | None
        The active MongoDB client, or None if not yet connected.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 27_017,
        username: Optional[str] = None,
        password: Optional[str] = None,
        db_name: str = "AAC",
        pool_settings: Optional[Dict[str, int]] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._db_name = db_name
        self._pool_settings = pool_settings or POOL_DEFAULTS
        self.client: Optional[MongoClient] = None
        self._connect()

    # ------------------------------------------------------------------
    def _connect(self) -> None:
        """Open (or reopen) the MongoClient with pool settings."""
        kwargs: Dict[str, Any] = {**self._pool_settings}
        if self._username and self._password:
            kwargs.update(
                username=self._username,
                password=self._password,
                authSource=self._db_name,
                authMechanism="SCRAM-SHA-1",
            )
        try:
            self.client = MongoClient(self._host, self._port, **kwargs)
            # Force a round-trip to verify the connection is alive
            self.client.admin.command("ping")
            logger.info(
                "ConnectionPool: connected to %s:%s (pool max=%s)",
                self._host,
                self._port,
                self._pool_settings.get("maxPoolSize"),
            )
        except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
            logger.error("ConnectionPool: failed to connect — %s", exc)
            self.client = None
            raise

    # ------------------------------------------------------------------
    def is_healthy(self) -> bool:
        """Return True if the server is reachable."""
        if self.client is None:
            return False
        try:
            self.client.admin.command("ping")
            return True
        except PyMongoError:
            return False

    # ------------------------------------------------------------------
    def reconnect(self) -> bool:
        """
        Attempt to reconnect after a failure.

        Returns
        -------
        bool
            True if reconnection succeeded, False otherwise.
        """
        logger.warning("ConnectionPool: attempting reconnect…")
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass
        try:
            self._connect()
            return True
        except PyMongoError:
            return False

    # ------------------------------------------------------------------
    def get_database(self) -> Optional[Database]:
        """Return the target Database object, reconnecting if needed."""
        if not self.is_healthy():
            if not self.reconnect():
                logger.error("ConnectionPool: reconnect failed; returning None")
                return None
        return self.client[self._db_name]   # type: ignore[index]

    # ------------------------------------------------------------------
    @contextmanager
    def session(self) -> Generator:
        """
        Context manager that yields a MongoDB ClientSession for
        multi-document atomic operations.

        Usage
        -----
        with pool.session() as session:
            collection.insert_one(doc, session=session)
        """
        if self.client is None:
            raise ConnectionFailure("No active MongoDB client")
        with self.client.start_session() as sess:
            yield sess

    # ------------------------------------------------------------------
    def close(self) -> None:
        """Close the underlying MongoClient and release all connections."""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("ConnectionPool: client closed")

    # ------------------------------------------------------------------
    def __del__(self) -> None:
        self.close()


# ---------------------------------------------------------------------------
# 2. Index Manager
# ---------------------------------------------------------------------------

class IndexManager:
    """
    Creates, verifies, and reports on MongoDB indexes for the 'animals'
    collection.

    Proper indexing is the single most impactful database-layer optimisation
    available.  Without indexes, every rescue-type query performs a full
    collection scan (COLLSCAN), which is O(n).  With the compound index
    defined in ANIMAL_INDEXES, MongoDB can satisfy the query with an
    index scan (IXSCAN), which is O(log n + k).
    """

    def __init__(self, collection: Collection) -> None:
        self._col = collection

    # ------------------------------------------------------------------
    def create_all(self) -> Dict[str, bool]:
        """
        Ensure all indexes in ANIMAL_INDEXES exist on the collection.

        Returns
        -------
        dict
            Mapping of index name → True (created/already exists) or False (error).
        """
        results: Dict[str, bool] = {}
        for spec in ANIMAL_INDEXES:
            name = spec["name"]
            try:
                self._col.create_index(
                    spec["keys"],
                    unique=spec["unique"],
                    name=name,
                    sparse=spec["sparse"],
                    background=True,    # non-blocking build
                )
                logger.info("IndexManager: index '%s' ensured", name)
                results[name] = True
            except OperationFailure as exc:
                logger.error("IndexManager: failed to create '%s' — %s", name, exc)
                results[name] = False
        return results

    # ------------------------------------------------------------------
    def list_indexes(self) -> List[Dict[str, Any]]:
        """
        Return a list of all existing indexes with their key patterns.

        Returns
        -------
        list[dict]
            Each dict has 'name' and 'key' keys.
        """
        indexes = []
        for idx in self._col.list_indexes():
            indexes.append({"name": idx["name"], "key": dict(idx["key"])})
        logger.info("IndexManager: found %d indexes", len(indexes))
        return indexes

    # ------------------------------------------------------------------
    def drop_index(self, name: str) -> bool:
        """
        Drop a single index by name.

        Args
        ----
        name : str
            Index name as stored in MongoDB.

        Returns
        -------
        bool
            True if dropped successfully, False on error.
        """
        try:
            self._col.drop_index(name)
            logger.info("IndexManager: dropped index '%s'", name)
            return True
        except OperationFailure as exc:
            logger.error("IndexManager: could not drop '%s' — %s", name, exc)
            return False

    # ------------------------------------------------------------------
    def verify_rescue_index(self) -> bool:
        """
        Confirm that the compound rescue index exists and is usable.

        Returns
        -------
        bool
            True if 'idx_rescue_compound' is present.
        """
        existing = {idx["name"] for idx in self.list_indexes()}
        present = "idx_rescue_compound" in existing
        if present:
            logger.info("IndexManager: rescue compound index verified ✓")
        else:
            logger.warning("IndexManager: rescue compound index is MISSING")
        return present


# ---------------------------------------------------------------------------
# 3. Schema Validator
# ---------------------------------------------------------------------------

class SchemaValidator:
    """
    Applies and manages MongoDB collection-level JSON Schema validation.

    MongoDB's built-in schema validation rejects invalid documents at write
    time (insert / update), preventing corrupt data from ever reaching the
    collection.  This is a database-layer defence that complements (not
    replaces) application-layer validation in utils.py.
    """

    def __init__(self, db: Database, collection_name: str = "animals") -> None:
        self._db = db
        self._col_name = collection_name

    # ------------------------------------------------------------------
    def apply(self, validation_level: str = "moderate") -> bool:
        """
        Apply ANIMAL_SCHEMA to the collection using collMod.

        ``validation_level`` options
        ----------------------------
        - ``"strict"``    — validate all inserts AND all updates.
        - ``"moderate"``  — validate inserts and updates to documents that
                            already satisfy the schema; existing invalid
                            documents are left untouched.

        Args
        ----
        validation_level : str
            MongoDB validation level.  Defaults to 'moderate' to avoid
            rejecting pre-existing documents that may violate the schema.

        Returns
        -------
        bool
            True if the schema was applied successfully.
        """
        try:
            # Create collection if it doesn't exist yet
            if self._col_name not in self._db.list_collection_names():
                self._db.create_collection(self._col_name)
                logger.info("SchemaValidator: created collection '%s'", self._col_name)

            self._db.command(
                "collMod",
                self._col_name,
                validator=ANIMAL_SCHEMA,
                validationLevel=validation_level,
                validationAction="error",   # reject violating writes
            )
            logger.info(
                "SchemaValidator: schema applied to '%s' (level=%s)",
                self._col_name,
                validation_level,
            )
            return True
        except OperationFailure as exc:
            logger.error("SchemaValidator: failed to apply schema — %s", exc)
            return False

    # ------------------------------------------------------------------
    def remove(self) -> bool:
        """
        Remove schema validation from the collection.

        Useful during bulk data imports where the source data does not yet
        conform to the schema.

        Returns
        -------
        bool
            True if validation was removed successfully.
        """
        try:
            self._db.command(
                "collMod",
                self._col_name,
                validator={},
                validationLevel="off",
            )
            logger.info("SchemaValidator: validation removed from '%s'", self._col_name)
            return True
        except OperationFailure as exc:
            logger.error("SchemaValidator: failed to remove validation — %s", exc)
            return False

    # ------------------------------------------------------------------
    def get_current_schema(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve the currently active validator for the collection.

        Returns
        -------
        dict | None
            The validator document, or None if validation is not set.
        """
        try:
            info = self._db.command("listCollections", filter={"name": self._col_name})
            for col in info.get("cursor", {}).get("firstBatch", []):
                opts = col.get("options", {})
                return opts.get("validator")
            return None
        except OperationFailure as exc:
            logger.error("SchemaValidator: could not retrieve schema — %s", exc)
            return None


# ---------------------------------------------------------------------------
# 4. Aggregation Pipeline Builder
# ---------------------------------------------------------------------------

class AggregationPipelines:
    """
    Pre-built, parameterised MongoDB aggregation pipelines.

    Moving data-processing logic into the database engine (rather than
    loading all documents into Python and processing them with Pandas) has
    two benefits:

    1. **Reduced network transfer** — only the aggregated result travels
       over the wire, not every raw document.
    2. **Server-side optimisation** — MongoDB can use index scans for the
       ``$match`` stage if appropriate indexes exist, dramatically reducing
       the documents examined.
    """

    def __init__(self, collection: Collection) -> None:
        self._col = collection

    # ------------------------------------------------------------------
    def breed_distribution(
        self, rescue_type_query: Optional[Dict[str, Any]] = None, top_n: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Return the top-N breeds and their counts for a given filter.

        Args
        ----
        rescue_type_query : dict | None
            A MongoDB match expression (e.g. the water-rescue query).
            If None, all documents are included.
        top_n : int
            Maximum number of breeds to return.

        Returns
        -------
        list[dict]
            Each item has ``breed`` and ``count`` keys, sorted descending.
        """
        pipeline: List[Dict[str, Any]] = []
        if rescue_type_query:
            pipeline.append({"$match": rescue_type_query})
        pipeline += [
            {"$group": {"_id": "$breed", "count": {"$sum": 1}}},
            {"$sort": {"count": DESCENDING}},
            {"$limit": top_n},
            {"$project": {"_id": 0, "breed": "$_id", "count": 1}},
        ]
        try:
            results = list(self._col.aggregate(pipeline))
            logger.info(
                "AggregationPipelines.breed_distribution: %d results", len(results)
            )
            return results
        except PyMongoError as exc:
            logger.error("breed_distribution pipeline failed: %s", exc)
            return []

    # ------------------------------------------------------------------
    def age_statistics(
        self, match: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Compute min, max, average, and standard deviation of age in weeks.

        Args
        ----
        match : dict | None
            Optional pre-filter.

        Returns
        -------
        dict
            Keys: ``min_weeks``, ``max_weeks``, ``avg_weeks``, ``std_dev``,
            ``count``.
        """
        pipeline: List[Dict[str, Any]] = []
        if match:
            pipeline.append({"$match": match})
        pipeline += [
            {
                "$match": {
                    "age_upon_outcome_in_weeks": {"$exists": True, "$type": "number"}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "min_weeks": {"$min": "$age_upon_outcome_in_weeks"},
                    "max_weeks": {"$max": "$age_upon_outcome_in_weeks"},
                    "avg_weeks": {"$avg": "$age_upon_outcome_in_weeks"},
                    "std_dev": {"$stdDevPop": "$age_upon_outcome_in_weeks"},
                    "count": {"$sum": 1},
                }
            },
            {"$project": {"_id": 0}},
        ]
        try:
            results = list(self._col.aggregate(pipeline))
            if results:
                stats = results[0]
                # Round floats for readability
                for key in ("avg_weeks", "std_dev"):
                    if key in stats and stats[key] is not None:
                        stats[key] = round(stats[key], 2)
                logger.info("AggregationPipelines.age_statistics: %s", stats)
                return stats
            return {}
        except PyMongoError as exc:
            logger.error("age_statistics pipeline failed: %s", exc)
            return {}

    # ------------------------------------------------------------------
    def rescue_type_summary(self) -> List[Dict[str, Any]]:
        """
        Return a breakdown of how many animals qualify for each rescue type,
        computed entirely in MongoDB using ``$facet``.

        Returns
        -------
        list[dict]
            Each item has ``rescue_type`` and ``count`` keys.
        """
        water_match = {
            "animal_type": "Dog",
            "breed": {
                "$in": [
                    "Labrador Retriever Mix",
                    "Newfoundland Mix",
                    "Portuguese Water Dog Mix",
                ]
            },
            "age_upon_outcome_in_weeks": {"$gte": 26, "$lte": 156},
        }
        mountain_match = {
            "animal_type": "Dog",
            "breed": {
                "$in": [
                    "German Shepherd Mix",
                    "Alaskan Malamute Mix",
                    "Old English Sheepdog Mix",
                    "Siberian Husky Mix",
                    "Rottweiler Mix",
                ]
            },
            "age_upon_outcome_in_weeks": {"$gte": 26, "$lte": 156},
        }
        disaster_match = {
            "animal_type": "Dog",
            "breed": {
                "$in": [
                    "Doberman Pinscher Mix",
                    "German Shorthaired Pointer Mix",
                    "Bloodhound Mix",
                    "Golden Retriever Mix",
                ]
            },
            "age_upon_outcome_in_weeks": {"$gte": 20, "$lte": 300},
        }

        pipeline = [
            {
                "$facet": {
                    "water": [
                        {"$match": water_match},
                        {"$count": "count"},
                    ],
                    "mountain": [
                        {"$match": mountain_match},
                        {"$count": "count"},
                    ],
                    "disaster": [
                        {"$match": disaster_match},
                        {"$count": "count"},
                    ],
                }
            },
            {
                "$project": {
                    "summary": {
                        "$concatArrays": [
                            {
                                "$map": {
                                    "input": "$water",
                                    "as": "w",
                                    "in": {
                                        "rescue_type": "Water Rescue",
                                        "count": "$$w.count",
                                    },
                                }
                            },
                            {
                                "$map": {
                                    "input": "$mountain",
                                    "as": "m",
                                    "in": {
                                        "rescue_type": "Mountain Rescue",
                                        "count": "$$m.count",
                                    },
                                }
                            },
                            {
                                "$map": {
                                    "input": "$disaster",
                                    "as": "d",
                                    "in": {
                                        "rescue_type": "Disaster Rescue",
                                        "count": "$$d.count",
                                    },
                                }
                            },
                        ]
                    }
                }
            },
            {"$unwind": "$summary"},
            {"$replaceRoot": {"newRoot": "$summary"}},
        ]

        try:
            results = list(self._col.aggregate(pipeline))
            logger.info(
                "AggregationPipelines.rescue_type_summary: %d types", len(results)
            )
            return results
        except PyMongoError as exc:
            logger.error("rescue_type_summary pipeline failed: %s", exc)
            return []

    # ------------------------------------------------------------------
    def outcome_type_distribution(self) -> List[Dict[str, Any]]:
        """
        Count documents grouped by ``outcome_type``, sorted descending.

        Returns
        -------
        list[dict]
            Each item has ``outcome_type`` and ``count`` keys.
        """
        pipeline = [
            {"$group": {"_id": "$outcome_type", "count": {"$sum": 1}}},
            {"$sort": {"count": DESCENDING}},
            {"$project": {"_id": 0, "outcome_type": "$_id", "count": 1}},
        ]
        try:
            results = list(self._col.aggregate(pipeline))
            logger.info(
                "AggregationPipelines.outcome_type_distribution: %d types", len(results)
            )
            return results
        except PyMongoError as exc:
            logger.error("outcome_type_distribution pipeline failed: %s", exc)
            return []


# ---------------------------------------------------------------------------
# 5. Query Performance Logger
# ---------------------------------------------------------------------------

class QueryPerformanceLogger:
    """
    Wraps MongoDB queries with explain-plan analysis and wall-clock timing.

    During development and testing, this class surfaces the query plan
    chosen by MongoDB (COLLSCAN vs IXSCAN) and the number of documents
    examined, making it easy to verify that indexes are being used.
    """

    def __init__(self, collection: Collection) -> None:
        self._col = collection

    # ------------------------------------------------------------------
    def explain_query(
        self, query: Dict[str, Any], verbosity: str = "executionStats"
    ) -> Dict[str, Any]:
        """
        Run MongoDB's explain() on a find query and return key statistics.

        Args
        ----
        query : dict
            The MongoDB filter document.
        verbosity : str
            One of 'queryPlanner', 'executionStats', 'allPlansExecution'.

        Returns
        -------
        dict
            Simplified explain stats: stage, index used, docs examined,
            docs returned, execution time (ms).
        """
        try:
            raw = self._col.find(query).explain(verbosity)
            plan = raw.get("queryPlanner", {}).get("winningPlan", {})
            exec_stats = raw.get("executionStats", {})

            summary = {
                "winning_stage": self._extract_stage(plan),
                "index_name": self._extract_index(plan),
                "docs_examined": exec_stats.get("totalDocsExamined", "N/A"),
                "docs_returned": exec_stats.get("nReturned", "N/A"),
                "execution_time_ms": exec_stats.get("executionTimeMillis", "N/A"),
            }
            log_level = (
                logging.WARNING
                if summary["winning_stage"] == "COLLSCAN"
                else logging.INFO
            )
            logger.log(
                log_level,
                "QueryPerformanceLogger: stage=%s index=%s "
                "examined=%s returned=%s time=%sms",
                summary["winning_stage"],
                summary["index_name"],
                summary["docs_examined"],
                summary["docs_returned"],
                summary["execution_time_ms"],
            )
            return summary
        except PyMongoError as exc:
            logger.error("explain_query failed: %s", exc)
            return {}

    # ------------------------------------------------------------------
    def timed_find(
        self,
        query: Dict[str, Any],
        projection: Optional[Dict[str, int]] = None,
        limit: int = 0,
    ) -> Dict[str, Any]:
        """
        Execute a find() and return both the results and elapsed time.

        Args
        ----
        query      : dict
        projection : dict | None
        limit      : int   — 0 means no limit

        Returns
        -------
        dict
            Keys: ``results`` (list), ``elapsed_ms`` (float),
            ``count`` (int).
        """
        start = time.perf_counter()
        try:
            cursor = self._col.find(query, projection)
            if limit > 0:
                cursor = cursor.limit(limit)
            results = list(cursor)
            elapsed = round((time.perf_counter() - start) * 1_000, 2)
            logger.info(
                "QueryPerformanceLogger.timed_find: %d docs in %s ms",
                len(results),
                elapsed,
            )
            return {"results": results, "elapsed_ms": elapsed, "count": len(results)}
        except PyMongoError as exc:
            logger.error("timed_find failed: %s", exc)
            return {"results": [], "elapsed_ms": 0.0, "count": 0}

    # ------------------------------------------------------------------
    @staticmethod
    def _extract_stage(plan: Dict[str, Any]) -> str:
        """Recursively find the innermost stage name in a query plan."""
        stage = plan.get("stage", "UNKNOWN")
        if "inputStage" in plan:
            return QueryPerformanceLogger._extract_stage(plan["inputStage"])
        return stage

    @staticmethod
    def _extract_index(plan: Dict[str, Any]) -> Optional[str]:
        """Extract the index name used, if any."""
        if "indexName" in plan:
            return plan["indexName"]
        if "inputStage" in plan:
            return QueryPerformanceLogger._extract_index(plan["inputStage"])
        return None


# ---------------------------------------------------------------------------
# 6. DatabaseManager  — top-level façade
# ---------------------------------------------------------------------------

class DatabaseManager:
    """
    Top-level façade that composes ConnectionPool, IndexManager,
    SchemaValidator, AggregationPipelines, and QueryPerformanceLogger
    into a single, easy-to-use interface for the Grazioso Salvare Dashboard.

    Usage
    -----
    mgr = DatabaseManager(username="aacuser", password="secret")
    mgr.setup()                        # create indexes + apply schema
    summary = mgr.pipelines.rescue_type_summary()
    stats   = mgr.pipelines.age_statistics()
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 27_017,
        username: Optional[str] = None,
        password: Optional[str] = None,
        db_name: str = "AAC",
        collection_name: str = "animals",
        pool_settings: Optional[Dict[str, int]] = None,
    ) -> None:
        self._db_name = db_name
        self._col_name = collection_name

        # 1. Connection pool
        self.pool = ConnectionPool(
            host=host,
            port=port,
            username=username,
            password=password,
            db_name=db_name,
            pool_settings=pool_settings,
        )

        db = self.pool.get_database()
        if db is None:
            raise ConnectionFailure("DatabaseManager: could not obtain database handle")

        col = db[collection_name]

        # 2. Sub-components
        self.indexes = IndexManager(col)
        self.schema = SchemaValidator(db, collection_name)
        self.pipelines = AggregationPipelines(col)
        self.perf = QueryPerformanceLogger(col)

        logger.info(
            "DatabaseManager: initialised for %s/%s", db_name, collection_name
        )

    # ------------------------------------------------------------------
    def setup(self, apply_schema: bool = True) -> Dict[str, Any]:
        """
        One-call setup: create indexes and optionally apply schema validation.

        Args
        ----
        apply_schema : bool
            Whether to apply JSON Schema validation.  Set False for initial
            bulk imports where data may not yet conform.

        Returns
        -------
        dict
            Result summary with keys 'indexes' and 'schema_applied'.
        """
        index_results = self.indexes.create_all()
        schema_ok = self.schema.apply() if apply_schema else False
        summary = {"indexes": index_results, "schema_applied": schema_ok}
        logger.info("DatabaseManager.setup complete: %s", summary)
        return summary

    # ------------------------------------------------------------------
    def health_check(self) -> Dict[str, Any]:
        """
        Return a snapshot of the current database health.

        Returns
        -------
        dict
            Keys: ``connected``, ``rescue_index_present``,
            ``index_count``, ``pool_max_size``.
        """
        connected = self.pool.is_healthy()
        rescue_idx = self.indexes.verify_rescue_index() if connected else False
        idx_list = self.indexes.list_indexes() if connected else []
        return {
            "connected": connected,
            "rescue_index_present": rescue_idx,
            "index_count": len(idx_list),
            "pool_max_size": POOL_DEFAULTS["maxPoolSize"],
        }

    # ------------------------------------------------------------------
    def close(self) -> None:
        """Release all database connections."""
        self.pool.close()
        logger.info("DatabaseManager: closed")

    def __del__(self) -> None:
        self.close()
