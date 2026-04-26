from __future__ import annotations

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
