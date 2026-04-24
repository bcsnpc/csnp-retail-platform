"""Run-state manifest — persisted between backfill and daily runs.

Design rule: if all Parquet files are deleted and only manifest.json
remains, tomorrow's daily run must be correct.  Keep only state
needed to resume; do not track per-record ingestion history.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from pydantic import BaseModel, Field

from csnp_retail.config import Scale


class TimelineState(BaseModel):
    backfill_start: date
    backfill_end: date
    fictional_date: date            # last date for which data has been generated
    daily_runs_completed: int = 0   # audit counter; never used for RNG


class IdWatermarks(BaseModel):
    """High-water marks for all surrogate / sequence keys.

    Daily runner starts new keys at watermark + 1 to guarantee
    no collisions across runs.
    """
    sale_key:        int = 0
    order_seq:       int = 0   # global monotonic; order_id = CSNP-{YYYY}-{seq:09d}
    return_key:      int = 0
    session_key:     int = 0
    customer_key:    int = 0
    customer_id_seq: int = 0   # numeric part of CUST-{seq:05d}
    event_key:       int = 0
    spend_key:       int = 0
    inventory_key:   int = 0


class Manifest(BaseModel):
    schema_version: str = "1"
    seed: int
    scale: Scale
    generated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC)
    )
    timeline: TimelineState
    id_watermarks: IdWatermarks
    patterns_module_version: str    # module_version() from patterns.py at generation time
    tables_written: list[str] = []
    row_counts: dict[str, int] = {}

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=2))

    @classmethod
    def load(cls, path: Path) -> Manifest:
        return cls.model_validate_json(path.read_text())
