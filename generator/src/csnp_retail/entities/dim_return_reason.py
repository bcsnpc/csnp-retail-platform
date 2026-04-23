"""dim_return_reason — 12-row static return reason table."""

from __future__ import annotations

import pandas as pd

_REASONS = [
    (1,  "Wrong size",               "Fit",                 True),
    (2,  "Did not fit as expected",  "Fit",                 True),
    (3,  "Poor quality",             "Quality",             True),
    (4,  "Defective / damaged",      "Quality",             False),
    (5,  "Not as described",         "Expectation",         True),
    (6,  "Changed mind",             "Customer Preference", False),
    (7,  "Found a better price",     "Price",               False),
    (8,  "Ordered wrong item",       "Customer Error",      False),
    (9,  "Received wrong item",      "Fulfillment Error",   False),
    (10, "Gift return",              "Gift",                False),
    (11, "Too late for occasion",    "Timing",              False),
    (12, "Style not as expected",    "Expectation",         True),
]

_COLUMNS = ["return_reason_key", "return_reason", "return_reason_group", "is_controllable"]


def build_dim_return_reason() -> pd.DataFrame:
    return pd.DataFrame(_REASONS, columns=_COLUMNS)
