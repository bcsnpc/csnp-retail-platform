from __future__ import annotations

from csnp_helpers.onelake import onelake_files_path

BASE = "onelake.dfs.fabric.microsoft.com"


class TestOnelakeFilesPath:
    def test_full_path_structure(self):
        result = onelake_files_path("CSNP_Dev", "CSNP_Bronze", "bronze/dim_date/dim_date.parquet")
        assert result == (
            "abfss://CSNP_Dev@onelake.dfs.fabric.microsoft.com/"
            "CSNP_Bronze.Lakehouse/Files/bronze/dim_date/dim_date.parquet"
        )

    def test_abfss_scheme(self):
        result = onelake_files_path("CSNP_Dev", "CSNP_Bronze", "x.parquet")
        assert result.startswith("abfss://")

    def test_workspace_in_authority(self):
        result = onelake_files_path("CSNP_Prod", "CSNP_Bronze", "x.parquet")
        assert f"abfss://CSNP_Prod@{BASE}/" in result

    def test_lakehouse_dot_lakehouse_in_path(self):
        result = onelake_files_path("CSNP_Dev", "CSNP_Silver", "x.parquet")
        assert "CSNP_Silver.Lakehouse/" in result

    def test_files_segment_present(self):
        result = onelake_files_path("CSNP_Dev", "CSNP_Bronze", "a/b/c.parquet")
        assert "/Files/a/b/c.parquet" in result

    def test_subpath_not_double_slashed(self):
        result = onelake_files_path("CSNP_Dev", "CSNP_Bronze", "a/b.parquet")
        assert "Files//a" not in result
