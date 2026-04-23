"""Typer CLI entry point.

Usage:
    python -m csnp_retail generate --mode backfill --scale xs --out ./data/
    python -m csnp_retail generate --mode daily --date 2026-04-20 --out ./data/
"""

from __future__ import annotations

import datetime
import logging
from pathlib import Path
from typing import Annotated, Optional

import typer

from csnp_retail.config import BACKFILL_END, BACKFILL_START, GeneratorConfig, Mode, Scale


def _parse_date(s: str | None) -> datetime.date | None:
    if s is None:
        return None
    return datetime.date.fromisoformat(s)

logging.basicConfig(level=logging.INFO, format="%(message)s")

app = typer.Typer(
    name="csnp-retail",
    help="CSNP & Co. synthetic retail data generator.",
    rich_markup_mode="rich",
    no_args_is_help=True,
)


@app.command("version")
def version_cmd() -> None:
    """Show installed version."""
    from csnp_retail import __version__

    typer.echo(f"csnp-retail {__version__}")


@app.command("generate")
def generate(
    mode: Annotated[Mode, typer.Option("--mode", help="[backfill|daily]")],
    scale: Annotated[Scale, typer.Option("--scale", help="xs / s / m / l")] = Scale.m,
    out: Annotated[Path, typer.Option("--out", help="Output directory")] = Path("./data"),
    seed: Annotated[int, typer.Option("--seed", help="RNG seed (deterministic)")] = 42,
    start: Annotated[Optional[str], typer.Option("--start", help="YYYY-MM-DD")] = None,
    end: Annotated[Optional[str], typer.Option("--end", help="YYYY-MM-DD")] = None,
    target_date: Annotated[Optional[str], typer.Option("--date", help="YYYY-MM-DD")] = None,
    seed_file: Annotated[Optional[Path], typer.Option("--seed-file")] = None,
) -> None:
    """Generate CSNP & Co. synthetic retail data."""
    from csnp_retail.runner import run_backfill, run_daily

    start_d = _parse_date(start) or BACKFILL_START
    end_d = _parse_date(end) or BACKFILL_END
    target_d = _parse_date(target_date)

    config = GeneratorConfig(
        mode=mode,
        scale=scale,
        seed=seed,
        start=start_d,
        end=end_d,
        out=out,
        seed_file=seed_file,
    )

    if mode == Mode.backfill:
        typer.echo(
            f"[CSNP] backfill  scale={scale.value}  seed={seed}"
            f"  {config.start} -> {config.end}  ->  {out}"
        )
        run_backfill(config)
        typer.echo("[CSNP] done.")

    elif mode == Mode.daily:
        if target_d is None:
            typer.echo("ERROR: --date is required for daily mode.", err=True)
            raise typer.Exit(1)
        typer.echo(f"[CSNP] daily  date={target_d}  scale={scale.value}  ->  {out}")
        run_daily(config, target_d)
        typer.echo("[CSNP] done.")
