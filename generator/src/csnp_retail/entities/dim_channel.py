"""dim_channel — 4-row static sales channel table."""

from __future__ import annotations

import pandas as pd

_CHANNELS = [
    (1, "Store", "Physical", False),
    (2, "Web", "Digital", True),
    (3, "App", "Digital", True),
    (4, "Marketplace", "Digital", True),
]

_COLUMNS = ["channel_key", "channel_name", "channel_type", "is_digital"]


def build_dim_channel() -> pd.DataFrame:
    return pd.DataFrame(_CHANNELS, columns=_COLUMNS)
