import hashlib

import networkx as nx
import pandas as pd

from src.data_quality_checks.location_grouping import (
    assign_proximity_groups,
    hash_id_str,
)


def test_hash_id_str_preserves_hashes_with_two_hex_letters():
    digest = hashlib.md5(b"1.25_0.5", usedforsecurity=False).hexdigest()

    assert hash_id_str(digest) == digest[:8]


def test_hash_id_str_makes_numeric_prefix_spreadsheet_safe():
    assert hash_id_str("12345678ab0000000000000000000000") == "ef345678"


def test_proximity_groups_have_stable_sorted_member_hash_and_counts():
    graph = nx.Graph()
    graph.add_edges_from(
        [
            ("school-c", "school-a"),
            ("school-b", "school-c"),
            ("school-a", "school-b"),
        ]
    )
    graph.add_node("school-isolated")

    result = assign_proximity_groups(graph).set_index("school_id_giga")
    expected_digest = hashlib.md5(
        b"school-a,school-b,school-c", usedforsecurity=False
    ).hexdigest()
    expected_group_id = hash_id_str(expected_digest)

    for school_id in ("school-a", "school-b", "school-c"):
        assert result.loc[school_id, "flag"] == 1
        assert result.loc[school_id, "group_id"] == expected_group_id
        assert result.loc[school_id, "count"] == 3

    assert result.loc["school-isolated", "flag"] == 0
    assert pd.isna(result.loc["school-isolated", "group_id"])
    assert pd.isna(result.loc["school-isolated", "count"])
