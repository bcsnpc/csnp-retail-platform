"""Pattern verification stubs — one test per planted pattern (§6 of design doc).

These will be implemented once fact tables are generated (next session).
Each test will: generate XS data → aggregate → assert the pattern is detectable.
"""

import pytest


@pytest.mark.skip(reason="Requires fact_sales — implemented in next session")
def test_texas_heat_event():
    """TX Outerwear revenue drops ≥50% Jun 15 – Aug 16 FY25 vs. prior period."""
    pass


@pytest.mark.skip(reason="Requires fact_sales — implemented in next session")
def test_channel_mix_shift():
    """Ecom share grows from ~24% (FY24 Q1) to ~41% (FY26 Q1)."""
    pass


@pytest.mark.skip(reason="Requires fact_sales + dim_customer")
def test_bfcm_cohort_degradation():
    """BFCM-acquired cohort has ≥25% lower 90-day retention than organic cohorts."""
    pass


@pytest.mark.skip(reason="Requires fact_sales")
def test_signature_sizing_issue():
    """Field Straight-Leg Signature pant return rate ≥20% vs. 9% overall."""
    pass


@pytest.mark.skip(reason="Requires fact_sales")
def test_meridian_cable_spike():
    """Meridian Cable Crew volume Jul 2025 is ≥5× the prior 3-month avg."""
    pass


@pytest.mark.skip(reason="Requires fact_sessions")
def test_conversion_funnel_by_device():
    """iOS app conversion ≥3.5%; mobile web conversion ≤2%."""
    pass
