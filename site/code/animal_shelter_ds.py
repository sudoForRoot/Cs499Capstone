"""
Animal Shelter Data Structures Module
CS 499 Capstone Project - Algorithms and Data Structures Enhancement

This module implements specialized data structures and algorithms to improve
the performance and scalability of the Grazioso Salvare Dashboard. It provides:
  - An in-memory index (hash map) for O(1) average-case lookups by animal ID
  - A breed-based inverted index for fast multi-criteria rescue queries
  - A min-heap priority queue to rank animals by suitability scores
  - Binary search for age-range filtering on sorted arrays

These structures replace or complement the naive linear-scan MongoDB queries,
dramatically reducing time complexity for repeated, in-session queries.

Author: Steven Foltz
Course: CS 499 Capstone
Date: March 22, 2026
"""

import heapq
import logging
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Hash-Map Index  (O(1) average lookup by animal_id)
# ---------------------------------------------------------------------------

class AnimalIndex:
    """
    An in-memory hash-map index keyed on a unique animal identifier.

    Time Complexity
    ---------------
    insert  : O(1) average
    lookup  : O(1) average
    delete  : O(1) average
    list_all: O(n)

    Space Complexity: O(n)
    """

    def __init__(self):
        self._store: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    def insert(self, animal_id: str, record: Dict[str, Any]) -> None:
        """
        Insert or overwrite an animal record.

        Args:
            animal_id (str): Unique key (e.g., MongoDB _id as string).
            record    (dict): Full animal document.
        """
        if not animal_id:
            raise ValueError("animal_id must be a non-empty string")
        self._store[animal_id] = record
        logger.debug("AnimalIndex.insert: id=%s", animal_id)

    # ------------------------------------------------------------------
    def lookup(self, animal_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a record by its ID.

        Args:
            animal_id (str): ID to look up.

        Returns:
            dict | None: The animal record, or None if not found.
        """
        return self._store.get(animal_id)

    # ------------------------------------------------------------------
    def delete(self, animal_id: str) -> bool:
        """
        Remove an animal record from the index.

        Args:
            animal_id (str): ID to remove.

        Returns:
            bool: True if the record existed and was removed, False otherwise.
        """
        if animal_id in self._store:
            del self._store[animal_id]
            logger.debug("AnimalIndex.delete: id=%s", animal_id)
            return True
        return False

    # ------------------------------------------------------------------
    def list_all(self) -> List[Dict[str, Any]]:
        """Return all records as a list. O(n)."""
        return list(self._store.values())

    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return len(self._store)


# ---------------------------------------------------------------------------
# 2. Inverted Index  (breed → set of animal_ids for O(1) breed lookup)
# ---------------------------------------------------------------------------

class BreedInvertedIndex:
    """
    Inverted index mapping normalised breed names to sets of animal IDs.

    Building the index is O(n).  Looking up animals by one or more breeds
    is O(k) where k is the result-set size — far faster than a full-table
    scan for repeated queries.

    Time Complexity
    ---------------
    build        : O(n)
    lookup_breed : O(k) where k = number of matching animals
    multi_lookup : O(b * k) where b = number of breeds queried

    Space Complexity: O(n)
    """

    def __init__(self):
        # breed_key -> set of animal_ids
        self._index: Dict[str, set] = defaultdict(set)

    # ------------------------------------------------------------------
    @staticmethod
    def _normalise(breed: str) -> str:
        """Lowercase + strip for consistent key comparison."""
        return breed.lower().strip()

    # ------------------------------------------------------------------
    def build(self, records: List[Dict[str, Any]],
              id_field: str = "animal_id") -> None:
        """
        Populate the index from a list of animal records.

        Args:
            records  (list): Animal documents from the database.
            id_field (str) : Field used as the unique key.
        """
        self._index.clear()
        for record in records:
            animal_id = str(record.get(id_field, id(record)))
            breed = record.get("breed", "")
            if breed:
                key = self._normalise(breed)
                self._index[key].add(animal_id)
        logger.info("BreedInvertedIndex built: %d breeds indexed", len(self._index))

    # ------------------------------------------------------------------
    def lookup_breed(self, breed: str) -> set:
        """
        Return the set of animal IDs for an exact breed match.

        Args:
            breed (str): Breed name to look up.

        Returns:
            set: Animal IDs with that breed (empty set if none).
        """
        return set(self._index.get(self._normalise(breed), set()))

    # ------------------------------------------------------------------
    def multi_lookup(self, breeds: List[str]) -> set:
        """
        Union of IDs across multiple breed names (OR semantics).

        Args:
            breeds (list[str]): Breed names to query.

        Returns:
            set: Union of all matching animal IDs.
        """
        result: set = set()
        for breed in breeds:
            result |= self.lookup_breed(breed)
        return result


# ---------------------------------------------------------------------------
# 3. Suitability Scoring & Min-Heap Priority Queue
# ---------------------------------------------------------------------------

def compute_suitability_score(record: Dict[str, Any],
                               rescue_type: str) -> float:
    """
    Compute a numeric suitability score for a given rescue type.

    Scoring formula
    ---------------
    Each factor contributes points; higher is more suitable.

    Water rescue ideal: Labrador / Newfoundland / Portuguese Water Dog,
                        age 26-156 weeks, sex intact female preferred.
    Mountain rescue ideal: German Shepherd / Husky / Malamute,
                           age 26-156 weeks, intact male or female.
    Disaster rescue ideal: Doberman / Bloodhound / Golden Retriever,
                           age 20-300 weeks.

    Args:
        record      (dict): Animal record.
        rescue_type (str) : One of 'water', 'mountain', 'disaster'.

    Returns:
        float: Suitability score (higher = more suitable, 0 = not suitable).
    """
    score: float = 0.0
    breed: str = record.get("breed", "").lower()
    age: float = float(record.get("age_upon_outcome_in_weeks", 0))
    sex: str = record.get("sex_upon_outcome", "").lower()

    # ---- breed score -------------------------------------------------------
    BREED_SCORES: Dict[str, Dict[str, float]] = {
        "water": {
            "labrador retriever mix": 30.0,
            "newfoundland mix": 28.0,
            "portuguese water dog mix": 26.0,
        },
        "mountain": {
            "german shepherd mix": 30.0,
            "siberian husky mix": 28.0,
            "alaskan malamute mix": 27.0,
            "old english sheepdog mix": 24.0,
            "rottweiler mix": 22.0,
        },
        "disaster": {
            "doberman pinscher mix": 30.0,
            "bloodhound mix": 29.0,
            "golden retriever mix": 27.0,
            "german shorthaired pointer mix": 25.0,
        },
    }

    breed_map = BREED_SCORES.get(rescue_type, {})
    score += breed_map.get(breed, 0.0)

    if score == 0.0:
        # Breed not in the approved list — not suitable
        return 0.0

    # ---- age score ---------------------------------------------------------
    AGE_RANGES: Dict[str, Tuple[float, float]] = {
        "water":    (26.0, 156.0),
        "mountain": (26.0, 156.0),
        "disaster": (20.0, 300.0),
    }
    low, high = AGE_RANGES.get(rescue_type, (0.0, 9999.0))
    if low <= age <= high:
        # Bonus: animals closer to the ideal midpoint score higher
        midpoint = (low + high) / 2.0
        age_score = 20.0 * (1.0 - abs(age - midpoint) / (high - low))
        score += age_score
    else:
        # Outside age range — disqualify
        return 0.0

    # ---- sex score (intact preferred for working dogs) --------------------
    if "intact" in sex:
        score += 10.0

    return round(score, 2)


class RescuePriorityQueue:
    """
    Min-heap priority queue that surfaces the TOP-N most suitable rescue
    candidates.

    Because Python's heapq is a min-heap, scores are stored as negatives so
    that heappop returns the highest-scoring animal first.

    Time Complexity
    ---------------
    push : O(log n)
    pop  : O(log n)
    top_n: O(k log n) where k = number of results requested

    Space Complexity: O(n)
    """

    def __init__(self):
        self._heap: List[Tuple[float, str, Dict[str, Any]]] = []

    # ------------------------------------------------------------------
    def push(self, score: float, animal_id: str,
             record: Dict[str, Any]) -> None:
        """
        Add an animal to the queue.

        Args:
            score     (float): Suitability score.
            animal_id (str)  : Unique identifier (used as tiebreaker).
            record    (dict) : Full animal document.
        """
        # Negate score so highest score is popped first (min-heap trick)
        heapq.heappush(self._heap, (-score, animal_id, record))

    # ------------------------------------------------------------------
    def pop(self) -> Optional[Tuple[float, Dict[str, Any]]]:
        """
        Remove and return the highest-scored animal.

        Returns:
            (score, record) tuple, or None if the queue is empty.
        """
        if not self._heap:
            return None
        neg_score, _, record = heapq.heappop(self._heap)
        return (-neg_score, record)

    # ------------------------------------------------------------------
    def top_n(self, n: int) -> List[Tuple[float, Dict[str, Any]]]:
        """
        Return the top-N animals by suitability without emptying the queue.

        Args:
            n (int): Number of results to return.

        Returns:
            list of (score, record) tuples, highest score first.
        """
        # heapq.nsmallest on negative scores = highest actual scores
        candidates = heapq.nsmallest(n, self._heap)
        return [(-neg_score, record) for neg_score, _, record in candidates]

    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return len(self._heap)


# ---------------------------------------------------------------------------
# 4. Binary Search for Age-Range Filtering on Sorted Arrays
# ---------------------------------------------------------------------------

def binary_search_left(sorted_ages: List[float], target: float) -> int:
    """
    Return the leftmost index where sorted_ages[i] >= target.

    Implements a standard binary search (lower-bound).

    Time Complexity : O(log n)
    Space Complexity: O(1)

    Args:
        sorted_ages (list[float]): Ages sorted in ascending order.
        target      (float)      : Lower bound.

    Returns:
        int: Index of the first element >= target.
    """
    lo, hi = 0, len(sorted_ages)
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_ages[mid] < target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def binary_search_right(sorted_ages: List[float], target: float) -> int:
    """
    Return the index one past the rightmost position where sorted_ages[i] <= target.

    Implements a standard binary search (upper-bound).

    Time Complexity : O(log n)
    Space Complexity: O(1)

    Args:
        sorted_ages (list[float]): Ages sorted in ascending order.
        target      (float)      : Upper bound.

    Returns:
        int: Index past the last element <= target.
    """
    lo, hi = 0, len(sorted_ages)
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_ages[mid] <= target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def filter_by_age_range(records: List[Dict[str, Any]],
                         min_age: float,
                         max_age: float) -> List[Dict[str, Any]]:
    """
    Filter a list of animal records by an age range using binary search.

    The records are sorted by age once — O(n log n) — then each range query
    runs in O(log n + k) rather than O(n), where k is the number of results.

    Args:
        records (list): Animal documents (need not be pre-sorted).
        min_age (float): Minimum age in weeks (inclusive).
        max_age (float): Maximum age in weeks (inclusive).

    Returns:
        list: Matching animal records.
    """
    if not records:
        return []

    # Sort by age — O(n log n)
    sorted_records = sorted(
        records,
        key=lambda r: float(r.get("age_upon_outcome_in_weeks", 0))
    )
    sorted_ages = [
        float(r.get("age_upon_outcome_in_weeks", 0)) for r in sorted_records
    ]

    # Binary search for range boundaries — O(log n) each
    left = binary_search_left(sorted_ages, min_age)
    right = binary_search_right(sorted_ages, max_age)

    logger.debug(
        "filter_by_age_range: range=[%s, %s], matched %d of %d records",
        min_age, max_age, right - left, len(records)
    )
    return sorted_records[left:right]


# ---------------------------------------------------------------------------
# 5. Rescue Query Engine — ties everything together
# ---------------------------------------------------------------------------

class RescueQueryEngine:
    """
    High-level query engine that combines the data structures above to answer
    'which animals are most suitable for rescue type X?' efficiently.

    Workflow
    --------
    1. Load all records into AnimalIndex and BreedInvertedIndex.
    2. For a given rescue_type, use the inverted index to get candidate IDs.
    3. Retrieve full records from AnimalIndex (O(1) each).
    4. Apply age-range filter via binary search.
    5. Score each candidate and push onto RescuePriorityQueue.
    6. Return top-N results from the heap.
    """

    # Approved breeds per rescue category (must match crud.py values)
    RESCUE_BREEDS: Dict[str, List[str]] = {
        "water": [
            "Labrador Retriever Mix",
            "Newfoundland Mix",
            "Portuguese Water Dog Mix",
        ],
        "mountain": [
            "German Shepherd Mix",
            "Alaskan Malamute Mix",
            "Old English Sheepdog Mix",
            "Siberian Husky Mix",
            "Rottweiler Mix",
        ],
        "disaster": [
            "Doberman Pinscher Mix",
            "German Shorthaired Pointer Mix",
            "Bloodhound Mix",
            "Golden Retriever Mix",
        ],
    }

    AGE_RANGES: Dict[str, Tuple[float, float]] = {
        "water":    (26.0, 156.0),
        "mountain": (26.0, 156.0),
        "disaster": (20.0, 300.0),
    }

    # ------------------------------------------------------------------
    def __init__(self, records: List[Dict[str, Any]],
                 id_field: str = "animal_id"):
        """
        Build all indexes from a list of animal records.

        Args:
            records  (list): All animal documents.
            id_field (str) : Field used as unique key.
        """
        self._id_field = id_field
        self._index = AnimalIndex()
        self._breed_index = BreedInvertedIndex()

        # Populate indexes — O(n) total
        for record in records:
            animal_id = str(record.get(id_field, id(record)))
            self._index.insert(animal_id, record)

        self._breed_index.build(records, id_field)
        logger.info(
            "RescueQueryEngine initialised with %d records", len(self._index)
        )

    # ------------------------------------------------------------------
    def query(self, rescue_type: str,
              top_n: int = 50) -> List[Tuple[float, Dict[str, Any]]]:
        """
        Return the top-N animals ranked by suitability for rescue_type.

        Args:
            rescue_type (str): 'water', 'mountain', or 'disaster'.
            top_n       (int): Maximum number of results to return.

        Returns:
            list of (score, record) tuples, highest score first.
        """
        if rescue_type not in self.RESCUE_BREEDS:
            logger.warning("Unknown rescue type: %s", rescue_type)
            return []

        breeds = self.RESCUE_BREEDS[rescue_type]
        min_age, max_age = self.AGE_RANGES[rescue_type]

        # Step 1: Candidate IDs from inverted index — O(b * k)
        candidate_ids = self._breed_index.multi_lookup(breeds)
        logger.info(
            "rescue_type=%s: %d breed candidates", rescue_type, len(candidate_ids)
        )

        # Step 2: Retrieve full records — O(k)
        candidates = [
            self._index.lookup(aid)
            for aid in candidate_ids
            if self._index.lookup(aid) is not None
        ]

        # Step 3: Age-range filter via binary search — O(k log k)
        age_filtered = filter_by_age_range(candidates, min_age, max_age)
        logger.info(
            "After age filter: %d candidates remain", len(age_filtered)
        )

        # Step 4: Score and push onto priority queue — O(k log k)
        pq = RescuePriorityQueue()
        for record in age_filtered:
            aid = str(record.get(self._id_field, id(record)))
            score = compute_suitability_score(record, rescue_type)
            if score > 0.0:
                pq.push(score, aid, record)

        # Step 5: Extract top-N — O(top_n log k)
        results = pq.top_n(top_n)
        logger.info(
            "query complete: rescue_type=%s, results=%d", rescue_type, len(results)
        )
        return results
