import pytest

from corpus_pipeline.audit import jaccard, shingles, synthetic_pairs
from corpus_pipeline.core import RECIPE, REVISION, SHARDS, canonical, catalog, identifier, implementation_id, read_ready, write_json
from corpus_pipeline.pipeline import release_identity
from corpus_pipeline.transform import content_split, normalize_text, text_audit_flags, text_hash


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


def test_v2_width_controls_and_preserved_content():
    assert normalize_text("\u3000ＡＢＣ１２３！\tＣａｆｅ\u0301\u3000\r\n") == " ABC123!\tCafé"
    kept = "繁體 简体  ① Ⅳ 𝑥 ± ∑ ½ ｶﾀｶﾅ\u200b\ufeff👩\u200d💻"
    assert normalize_text(kept) == kept
    raw = "\x00中文\r\n\t正文\x7f\x85"
    assert normalize_text(raw) == "中文\n\t正文"
    assert text_audit_flags(raw, normalize_text(raw)) == "control_chars_removed"
    assert text_audit_flags("中文\r\n\t正文\r末行", "中文\n\t正文\n末行") == ""
    assert normalize_text("\x00\x7f\x85\t\n") == ""
    assert normalize_text(normalize_text(raw)) == normalize_text(raw)


@pytest.mark.parametrize(("text", "flag"), [
    ("中文AB", ""),  # Exactly 0.5 is retained without the low-ratio flag.
    ("中ABC", "low_chinese_ratio"),
    ("𠀀𰀀AB", ""),  # CJK extensions B and G in Python 3.11's Unicode database.
    ("中文éΩ123!?", ""),  # Unicode letters, not just ASCII, form the denominator.
    ("中éΩ123!?", "low_chinese_ratio"),
    ("123！？👩\u200d💻", "language_undetermined"),
])
def test_language_audit_boundaries(text, flag):
    cleaned = normalize_text(text)
    assert text_audit_flags(text, cleaned) == flag


def test_v2_normalization_drives_content_identity():
    left = normalize_text("ＡＢＣ\u3000Ｃａｆｅ\u0301\x00")
    right = normalize_text("ABC Café")
    assert left == right
    assert text_hash(left) == text_hash(right)
    assert content_split(text_hash(left)) == content_split(text_hash(right))


def test_mixed_recipe_batches_are_rejected():
    manifests = [
        {"batch_id": "old", "identity": {"source_kind": "synthetic", "revision": "synthetic-v1",
          "scope": "full", "recipe_version": "recipe_v1", "implementation_id": "old-implementation"}},
        {"batch_id": "new", "identity": {"source_kind": "synthetic", "revision": "synthetic-v1",
          "scope": "full", "recipe_version": RECIPE, "implementation_id": implementation_id()}},
    ]
    assert release_identity(manifests[1:])["recipe_version"] == RECIPE
    with pytest.raises(ValueError, match="Re-clean batches"):
        release_identity(manifests)


def test_previous_ready_recipe_remains_readable(tmp_path):
    directory = tmp_path / "releases" / "historical-v1"
    identity = {"recipe_version": "recipe_v1", "implementation_id": "historical-implementation"}
    manifest = {"release_id": "historical-v1", "identity": identity, "report": {"accepted_count": 1}}
    write_json(directory / "manifest.json", manifest)
    with catalog(tmp_path) as connection:
        connection.execute("INSERT INTO releases VALUES (?,?,?,?, 'READY')",
                           ["historical-v1", canonical(identity), str(directory), canonical(manifest["report"])])
    assert read_ready(tmp_path, "historical-v1") == {**manifest, "directory": str(directory)}


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
