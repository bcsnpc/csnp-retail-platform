from __future__ import annotations


def onelake_files_path(workspace: str, lakehouse: str, subpath: str) -> str:
    """Build an ABFSS URI for a file under a Fabric lakehouse Files/ section."""
    return (
        f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse}.Lakehouse/Files/{subpath}"
    )
