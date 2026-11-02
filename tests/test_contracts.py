import pytest

from corpus_pipeline.audit import jaccard, shingles, synthetic_pairs
from corpus_pipeline.core import REVISION, SHARDS, identifier, read_ready
from corpus_pipeline.transform import content_split, normalize_text, text_hash


def test_conservative_normalization():
    assert normalize_text("\r\n  if x:  \r\n    print('中文')\t\r\n\r\n\r\nCafé\n") == "  if x:\n    print('中文')\n\nCafé"
    assert normalize_text("Cafe\u0301") == "Café"
    assert normalize_text("繁體 不等於 简体  A!=a  x < 1") == "繁體 不等於 简体  A!=a  x < 1"
    assert normalize_text(" \t\r\n\u2003") == ""
    assert normalize_text(None) is None
    sample = "  第一行\n\n\n第二行 \n"
    assert normalize_text(normalize_text(sample)) == normalize_text(sample)


def test_content_hash_and_split_are_stable():
    left = text_hash(normalize_text("Cafe\u0301 \r\n"))
    right = text_hash(normalize_text("Café"))
    assert left == right
    assert content_split(left) == content_split(right)
    assert content_split("00000000" + "0" * 56) == "validation"
    assert content_split("00000005" + "0" * 56) == "train"


@pytest.mark.parametrize("name", ["../release", "/tmp/data", ".", "", "x/y", "..\\release"])
def test_path_identifiers_reject_traversal(name):
    with pytest.raises(ValueError):
        identifier(name)


def test_fixed_source_and_readiness(tmp_path):
    assert len(REVISION) == 40
    assert len(SHARDS) == 6
    # A directory alone must never become a consumer-visible release.
    (tmp_path / "releases" / "r001").mkdir(parents=True)
    with pytest.raises(ValueError, match="not READY"):
        read_ready(tmp_path, "r001")


def test_audit_ground_truth_is_bounded_and_not_semantic_recall():
    pairs = synthetic_pairs()
    assert len(pairs) == 40
    assert sum(pair["expected_near"] for pair in pairs) == 20
    for pair in pairs:
        assert (jaccard(pair["left"], pair["right"]) >= 0.8) == pair["expected_near"]
    assert shingles("abcde") == {"abcde"}
