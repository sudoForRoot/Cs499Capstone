"""
Unit Tests - Algorithms and Data Structures Enhancement
CS 499 Capstone Project

Tests for animal_shelter_ds.py covering:
  - AnimalIndex  (hash-map)
  - BreedInvertedIndex
  - RescuePriorityQueue
  - Binary search age-range filter
  - RescueQueryEngine (integration)

Run with:  python -m pytest test_animal_shelter_ds.py -v

Author: Steven Foltz
Course: CS 499 Capstone
Date: March 22, 2026
"""

import pytest
from animal_shelter_ds import (
    AnimalIndex,
    BreedInvertedIndex,
    RescuePriorityQueue,
    compute_suitability_score,
    filter_by_age_range,
    binary_search_left,
    binary_search_right,
    RescueQueryEngine,
)

# ─────────────────────────── fixtures ────────────────────────────────────────

SAMPLE_RECORDS = [
    {
        "animal_id": "A001",
        "name": "Buddy",
        "animal_type": "Dog",
        "breed": "Labrador Retriever Mix",
        "age_upon_outcome_in_weeks": 52,
        "sex_upon_outcome": "Intact Male",
    },
    {
        "animal_id": "A002",
        "name": "Luna",
        "animal_type": "Dog",
        "breed": "German Shepherd Mix",
        "age_upon_outcome_in_weeks": 78,
        "sex_upon_outcome": "Intact Female",
    },
    {
        "animal_id": "A003",
        "name": "Max",
        "animal_type": "Dog",
        "breed": "Golden Retriever Mix",
        "age_upon_outcome_in_weeks": 104,
        "sex_upon_outcome": "Neutered Male",
    },
    {
        "animal_id": "A004",
        "name": "Daisy",
        "animal_type": "Dog",
        "breed": "Labrador Retriever Mix",
        "age_upon_outcome_in_weeks": 200,   # too old for water rescue
        "sex_upon_outcome": "Spayed Female",
    },
    {
        "animal_id": "A005",
        "name": "Charlie",
        "animal_type": "Cat",               # not a dog — not rescue-eligible
        "breed": "Domestic Shorthair Mix",
        "age_upon_outcome_in_weeks": 30,
        "sex_upon_outcome": "Intact Female",
    },
]


# ─────────────────────────── AnimalIndex ─────────────────────────────────────

class TestAnimalIndex:
    def test_insert_and_lookup(self):
        idx = AnimalIndex()
        idx.insert("A001", SAMPLE_RECORDS[0])
        result = idx.lookup("A001")
        assert result is not None
        assert result["name"] == "Buddy"

    def test_lookup_missing(self):
        idx = AnimalIndex()
        assert idx.lookup("NONEXISTENT") is None

    def test_delete_existing(self):
        idx = AnimalIndex()
        idx.insert("A001", SAMPLE_RECORDS[0])
        removed = idx.delete("A001")
        assert removed is True
        assert idx.lookup("A001") is None

    def test_delete_missing(self):
        idx = AnimalIndex()
        assert idx.delete("NONEXISTENT") is False

    def test_len(self):
        idx = AnimalIndex()
        for r in SAMPLE_RECORDS:
            idx.insert(r["animal_id"], r)
        assert len(idx) == len(SAMPLE_RECORDS)

    def test_list_all(self):
        idx = AnimalIndex()
        for r in SAMPLE_RECORDS:
            idx.insert(r["animal_id"], r)
        all_records = idx.list_all()
        assert len(all_records) == len(SAMPLE_RECORDS)

    def test_insert_empty_id_raises(self):
        idx = AnimalIndex()
        with pytest.raises(ValueError):
            idx.insert("", SAMPLE_RECORDS[0])

    def test_overwrite(self):
        idx = AnimalIndex()
        idx.insert("A001", SAMPLE_RECORDS[0])
        updated = dict(SAMPLE_RECORDS[0])
        updated["name"] = "Buddy Updated"
        idx.insert("A001", updated)
        assert idx.lookup("A001")["name"] == "Buddy Updated"
        assert len(idx) == 1


# ─────────────────────────── BreedInvertedIndex ──────────────────────────────

class TestBreedInvertedIndex:
    def setup_method(self):
        self.bidx = BreedInvertedIndex()
        self.bidx.build(SAMPLE_RECORDS, id_field="animal_id")

    def test_lookup_exact_breed(self):
        ids = self.bidx.lookup_breed("Labrador Retriever Mix")
        assert "A001" in ids
        assert "A004" in ids

    def test_lookup_case_insensitive(self):
        ids = self.bidx.lookup_breed("LABRADOR RETRIEVER MIX")
        assert "A001" in ids

    def test_lookup_missing_breed(self):
        ids = self.bidx.lookup_breed("Poodle Mix")
        assert len(ids) == 0

    def test_multi_lookup_union(self):
        ids = self.bidx.multi_lookup(["Labrador Retriever Mix", "German Shepherd Mix"])
        assert "A001" in ids
        assert "A002" in ids
        assert "A004" in ids

    def test_rebuild_clears_old_data(self):
        self.bidx.build(SAMPLE_RECORDS[:1], id_field="animal_id")
        ids = self.bidx.lookup_breed("German Shepherd Mix")
        assert len(ids) == 0


# ─────────────────────────── Suitability Score ───────────────────────────────

class TestComputeSuitabilityScore:
    def test_water_rescue_eligible(self):
        score = compute_suitability_score(SAMPLE_RECORDS[0], "water")
        assert score > 0

    def test_wrong_breed_scores_zero(self):
        score = compute_suitability_score(SAMPLE_RECORDS[1], "water")  # GSD -> not water
        assert score == 0.0

    def test_out_of_age_range_scores_zero(self):
        score = compute_suitability_score(SAMPLE_RECORDS[3], "water")  # 200 weeks
        assert score == 0.0

    def test_intact_bonus(self):
        intact = dict(SAMPLE_RECORDS[0])
        intact["sex_upon_outcome"] = "Intact Male"
        neutered = dict(SAMPLE_RECORDS[0])
        neutered["sex_upon_outcome"] = "Neutered Male"
        assert compute_suitability_score(intact, "water") > compute_suitability_score(neutered, "water")

    def test_unknown_rescue_type(self):
        score = compute_suitability_score(SAMPLE_RECORDS[0], "unknown")
        assert score == 0.0

    def test_disaster_rescue_eligible(self):
        score = compute_suitability_score(SAMPLE_RECORDS[2], "disaster")
        assert score > 0


# ─────────────────────────── RescuePriorityQueue ────────────────────────────

class TestRescuePriorityQueue:
    def test_push_and_pop_order(self):
        pq = RescuePriorityQueue()
        pq.push(10.0, "A", {"name": "Low"})
        pq.push(50.0, "B", {"name": "High"})
        pq.push(30.0, "C", {"name": "Mid"})

        score, record = pq.pop()
        assert score == pytest.approx(50.0)
        assert record["name"] == "High"

    def test_top_n_returns_correct_count(self):
        pq = RescuePriorityQueue()
        for i in range(10):
            pq.push(float(i), str(i), {"name": str(i)})
        results = pq.top_n(3)
        assert len(results) == 3
        # Top score should be 9
        assert results[0][0] == pytest.approx(9.0)

    def test_top_n_does_not_drain_queue(self):
        pq = RescuePriorityQueue()
        pq.push(5.0, "X", {})
        pq.push(3.0, "Y", {})
        _ = pq.top_n(2)
        assert len(pq) == 2

    def test_pop_empty_returns_none(self):
        pq = RescuePriorityQueue()
        assert pq.pop() is None

    def test_len(self):
        pq = RescuePriorityQueue()
        pq.push(1.0, "A", {})
        pq.push(2.0, "B", {})
        assert len(pq) == 2


# ─────────────────────────── Binary Search ───────────────────────────────────

class TestBinarySearch:
    def setup_method(self):
        self.ages = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]

    def test_left_found(self):
        assert binary_search_left(self.ages, 30.0) == 2

    def test_left_below_range(self):
        assert binary_search_left(self.ages, 0.0) == 0

    def test_left_above_range(self):
        assert binary_search_left(self.ages, 100.0) == len(self.ages)

    def test_right_found(self):
        assert binary_search_right(self.ages, 50.0) == 5

    def test_right_above_range(self):
        assert binary_search_right(self.ages, 100.0) == len(self.ages)

    def test_right_below_range(self):
        assert binary_search_right(self.ages, 0.0) == 0


class TestFilterByAgeRange:
    def test_basic_filter(self):
        results = filter_by_age_range(SAMPLE_RECORDS, 26.0, 156.0)
        names = [r["name"] for r in results]
        # Buddy (52), Luna (78), Max (104) qualify; Daisy (200) and Charlie (30) ...
        # Charlie is 30 weeks so also in range
        assert "Buddy" in names
        assert "Luna" in names
        assert "Max" in names
        assert "Daisy" not in names  # 200 > 156

    def test_empty_input(self):
        assert filter_by_age_range([], 0.0, 100.0) == []

    def test_no_matches(self):
        results = filter_by_age_range(SAMPLE_RECORDS, 500.0, 600.0)
        assert results == []

    def test_results_sorted_by_age(self):
        results = filter_by_age_range(SAMPLE_RECORDS, 0.0, 9999.0)
        ages = [r["age_upon_outcome_in_weeks"] for r in results]
        assert ages == sorted(ages)


# ─────────────────────────── RescueQueryEngine ───────────────────────────────

class TestRescueQueryEngine:
    def setup_method(self):
        self.engine = RescueQueryEngine(SAMPLE_RECORDS, id_field="animal_id")

    def test_water_query_returns_buddy_not_daisy(self):
        results = self.engine.query("water")
        ids = [r["animal_id"] for _, r in results]
        assert "A001" in ids       # Buddy: Lab, 52 weeks ✓
        assert "A004" not in ids   # Daisy: Lab but 200 weeks ✗

    def test_mountain_query(self):
        results = self.engine.query("mountain")
        ids = [r["animal_id"] for _, r in results]
        assert "A002" in ids       # Luna: GSD, 78 weeks ✓

    def test_disaster_query(self):
        results = self.engine.query("disaster")
        ids = [r["animal_id"] for _, r in results]
        assert "A003" in ids       # Max: Golden Retriever, 104 weeks ✓

    def test_results_sorted_descending_by_score(self):
        results = self.engine.query("water")
        scores = [s for s, _ in results]
        assert scores == sorted(scores, reverse=True)

    def test_top_n_limit(self):
        results = self.engine.query("water", top_n=1)
        assert len(results) <= 1

    def test_unknown_rescue_type(self):
        results = self.engine.query("unknown")
        assert results == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
