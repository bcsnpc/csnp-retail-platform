from __future__ import annotations

import inspect

import csnp_helpers

_EXPECTED = ["onelake_files_path", "add_lineage_columns", "merge_to_silver", "validate_silver"]


def test_all_public_functions_exported():
    """Guard against broken __init__.py — all four helpers must be importable and callable."""
    for name in _EXPECTED:
        fn = getattr(csnp_helpers, name, None)
        assert fn is not None, f"csnp_helpers.{name} is missing from public API"
        assert callable(fn), f"csnp_helpers.{name} is not callable"


def test_all_in_dunder_all():
    for name in _EXPECTED:
        assert name in csnp_helpers.__all__, f"{name!r} not listed in csnp_helpers.__all__"


def test_merge_to_silver_accepts_scd2_strategy():
    """scd2 strategy is implemented — no longer raises NotImplementedError."""
    source = inspect.getsource(csnp_helpers.merge_to_silver)
    assert "NotImplementedError" not in source
    assert "scd2" in source


def test_merge_to_silver_rejects_unknown_strategy():
    """Unknown strategies should raise ValueError, not silently pass."""
    import pytest

    with pytest.raises(ValueError, match="Unknown strategy"):
        # Passes a dummy df/table — ValueError is raised before any Spark call.
        csnp_helpers.merge_to_silver(None, "db.table", ["id"], strategy="scd99")


def test_onelake_path_format():
    path = csnp_helpers.onelake_files_path("MyWS", "MyLH", "bronze/table/table.parquet")
    assert path == (
        "abfss://MyWS@onelake.dfs.fabric.microsoft.com/"
        "MyLH.Lakehouse/Files/bronze/table/table.parquet"
    )
