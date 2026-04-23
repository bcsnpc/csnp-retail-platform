"""dim_date builder.

Fiscal year convention: April 1 start, named after the calendar year it ends in.
  April 2023 → FY2024 Q1  |  March 2024 → FY2024 Q4
  April 2024 → FY2025 Q1  |  March 2026 → FY2026 Q4
"""

from __future__ import annotations

import datetime
from typing import Generator

import holidays
import pandas as pd

_FISCAL_START_MONTH = 4  # April


def _fiscal_year(d: datetime.date) -> int:
    return d.year + 1 if d.month >= _FISCAL_START_MONTH else d.year


def _fiscal_month(d: datetime.date) -> int:
    """1 = April … 12 = March."""
    return (d.month - _FISCAL_START_MONTH) % 12 + 1


def _fiscal_quarter(fiscal_month: int) -> int:
    return (fiscal_month - 1) // 3 + 1


def _fiscal_week(d: datetime.date) -> int:
    """Week-of-fiscal-year (1-based, weeks start Monday)."""
    fy = _fiscal_year(d)
    fy_start = datetime.date(fy - 1, _FISCAL_START_MONTH, 1)
    return (d - fy_start).days // 7 + 1


def _season(d: datetime.date) -> str:
    m = d.month
    if m in (3, 4, 5):
        return "Spring"
    if m in (6, 7, 8):
        return "Summer"
    if m in (9, 10, 11):
        return "Fall"
    return "Winter"


def _is_month_end(d: datetime.date) -> bool:
    return d.month != (d + datetime.timedelta(days=1)).month


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> datetime.date:
    """Return the nth (0-based) occurrence of weekday (0=Mon) in year/month."""
    first = datetime.date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + datetime.timedelta(days=offset + 7 * n)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> datetime.date:
    next_month = month % 12 + 1
    next_year = year + (1 if month == 12 else 0)
    last_day = datetime.date(next_year, next_month, 1) - datetime.timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - datetime.timedelta(days=offset)


def _daterange(start: datetime.date, end: datetime.date) -> Generator[datetime.date, None, None]:
    d = start
    while d <= end:
        yield d
        d += datetime.timedelta(days=1)


def _build_holiday_index(
    start: datetime.date, end: datetime.date
) -> dict[datetime.date, str]:
    combined: dict[datetime.date, str] = {}
    years = range(start.year, end.year + 1)
    for country in ("US", "CA", "MX", "GB"):
        try:
            hols = holidays.country_holidays(country, years=years)
            for dt, name in hols.items():
                if dt not in combined:
                    combined[dt] = name
        except Exception:
            pass
    return combined


def _build_promo_index(
    start: datetime.date, end: datetime.date
) -> dict[datetime.date, str]:
    promo: dict[datetime.date, str] = {}

    def mark(date_start: datetime.date, date_end: datetime.date, name: str) -> None:
        for d in _daterange(date_start, date_end):
            promo.setdefault(d, name)

    for year in range(start.year, end.year + 1):
        # Black Friday / Cyber Monday: Thanksgiving (4th Thu Nov) + 1 → +4
        thanksgiving = _nth_weekday(year, 11, 3, 3)  # 4th Thursday (0-based n=3)
        mark(thanksgiving + datetime.timedelta(1), thanksgiving + datetime.timedelta(4),
             "Black Friday / Cyber Monday")

        # Mother's Day: 2 weeks prior through Mother's Day (2nd Sunday of May)
        mothers_day = _nth_weekday(year, 5, 6, 1)  # 2nd Sunday
        mark(mothers_day - datetime.timedelta(14), mothers_day, "Mother's Day")

        # Valentine's Day
        mark(datetime.date(year, 2, 7), datetime.date(year, 2, 14), "Valentine's Day")

        # Memorial Day weekend (last Monday of May, -2 days)
        memorial_day = _last_weekday_of_month(year, 5, 0)
        mark(memorial_day - datetime.timedelta(2), memorial_day, "Memorial Day")

        # Labor Day weekend (1st Monday of September, -2 days)
        labor_day = _nth_weekday(year, 9, 0, 0)
        mark(labor_day - datetime.timedelta(2), labor_day, "Labor Day")

        # Back-to-School
        mark(datetime.date(year, 7, 20), datetime.date(year, 9, 10), "Back-to-School")

        # Spring New Arrivals
        mark(datetime.date(year, 4, 1), datetime.date(year, 4, 15), "Spring New Arrivals")

        # Summer Sale
        mark(datetime.date(year, 6, 15), datetime.date(year, 7, 10), "Summer Sale")

        # Fall New Arrivals
        mark(datetime.date(year, 9, 1), datetime.date(year, 9, 15), "Fall New Arrivals")

        # Holiday Season
        mark(datetime.date(year, 12, 1), datetime.date(year, 12, 24), "Holiday Season")

        # New Year's Sale
        mark(datetime.date(year, 12, 26), datetime.date(year, 12, 31), "New Year's Sale")
        if year < end.year:
            mark(datetime.date(year + 1, 1, 1), datetime.date(year + 1, 1, 6),
                 "New Year's Sale")

        # End-of-Season Clearance (Mar 1-31)
        mark(datetime.date(year, 3, 1), datetime.date(year, 3, 31), "End-of-Season Clearance")

    return promo


def build_dim_date(
    start: datetime.date = datetime.date(2023, 4, 1),
    end: datetime.date = datetime.date(2026, 3, 31),
) -> pd.DataFrame:
    """Build the dim_date dimension table.

    Returns one row per calendar day in [start, end], with Gregorian,
    fiscal, holiday, promo, and seasonality attributes.
    """
    holiday_idx = _build_holiday_index(start, end)
    promo_idx = _build_promo_index(start, end)

    rows = []
    for ts in pd.date_range(start, end, freq="D"):
        d: datetime.date = ts.date()
        fm = _fiscal_month(d)
        fq = _fiscal_quarter(fm)
        fy = _fiscal_year(d)
        fw = _fiscal_week(d)

        is_me = _is_month_end(d)
        is_hol = d in holiday_idx
        is_promo = d in promo_idx
        is_weekend = d.weekday() >= 5
        fy_start = datetime.date(fy - 1, _FISCAL_START_MONTH, 1)

        rows.append({
            "date_key": int(d.strftime("%Y%m%d")),
            "date": d,
            "year": d.year,
            "month": d.month,
            "month_name": ts.strftime("%B"),
            "month_abbr": ts.strftime("%b"),
            "day": d.day,
            "day_of_year": d.timetuple().tm_yday,
            "day_of_week": d.weekday(),        # 0 = Monday
            "day_of_week_name": ts.strftime("%A"),
            "day_of_week_abbr": ts.strftime("%a"),
            "week_of_year": int(ts.strftime("%V")),   # ISO 8601
            "is_weekend": is_weekend,
            "quarter": (d.month - 1) // 3 + 1,
            "quarter_name": f"Q{(d.month - 1) // 3 + 1}",
            "is_month_end": is_me,
            "is_quarter_end": d.month in (3, 6, 9, 12) and is_me,
            "is_year_end": d.month == 12 and is_me,
            # Fiscal
            "fiscal_year": fy,
            "fiscal_quarter": fq,
            "fiscal_month": fm,
            "fiscal_week": fw,
            "day_of_fiscal_year": (d - fy_start).days + 1,
            "is_fiscal_month_end": is_me,       # fiscal months = calendar months
            "is_fiscal_quarter_end": d.month in (6, 9, 12, 3) and is_me,
            "is_fiscal_year_end": d.month == 3 and is_me,
            # Holidays / promos
            "is_holiday": is_hol,
            "holiday_name": holiday_idx.get(d),
            "is_business_day": not is_weekend and not is_hol,
            "is_promo_window": is_promo,
            "promo_name": promo_idx.get(d),
            # Seasonality / payroll
            "season": _season(d),
            "is_post_payday": d.day in (1, 15, 16),
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    # Boolean columns as proper bool dtype
    bool_cols = [c for c in df.columns if c.startswith("is_")]
    df[bool_cols] = df[bool_cols].astype(bool)
    return df
