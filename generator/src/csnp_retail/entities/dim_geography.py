"""dim_geography builder.

Returns one row per postal code in the curated CSNP & Co. operating geography.
This is a static reference table — same rows regardless of scale.
"""

from __future__ import annotations

import pandas as pd

from csnp_retail.faker_pools import GEO_COLUMNS, GEO_RECORDS


def build_dim_geography() -> pd.DataFrame:
    """Build dim_geography from the curated pool.

    Returns all ~350 postal records across US / CA / UK / MX.
    geo_key is a 1-based integer surrogate key.
    """
    df = pd.DataFrame(GEO_RECORDS, columns=GEO_COLUMNS)
    df.insert(0, "geo_key", range(1, len(df) + 1))

    # Type enforcement
    df["latitude"] = df["latitude"].astype("float64")
    df["longitude"] = df["longitude"].astype("float64")
    df["tax_rate"] = df["tax_rate"].astype("float64")
    df["geo_key"] = df["geo_key"].astype("int32")

    return df
