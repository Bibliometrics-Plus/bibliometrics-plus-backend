"""
Centralized SQL queries used by the final Streamlit dashboard.

All pages pull from this module so the dashboard remains easier to maintain and
the SQL can be reviewed in one place.
"""

from __future__ import annotations

import pandas as pd

from app.services.db import run_query
from app.services.filters import DashboardFilters


def _library_where(filters: DashboardFilters) -> tuple[str, dict]:
    """
    Build reusable library-level SQL filters.

    Most dashboard queries use the `library` table as the anchor point, so this
    helper avoids repeating the same conditional logic everywhere.
    """
    clauses: list[str] = []
    params: dict[str, object] = {}

    if not filters.is_all_systems:
        clauses.append("COALESCE(l.system_name, 'Unassigned') = :system_name")
        params["system_name"] = filters.system

    if not filters.is_all_branches:
        clauses.append("l.name = :branch_name")
        params["branch_name"] = filters.branch

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


def get_home_summary() -> pd.DataFrame:
    """Return high-level summary metrics for the landing page."""
    return run_query(
        """
        SELECT
            (SELECT COUNT(*) FROM library WHERE system_name IS NOT NULL) AS libraries,
            (SELECT COUNT(*) FROM collection_item) AS collection_items,
            (SELECT COUNT(*) FROM branch_kpi) AS branch_kpi_rows,
            (SELECT COUNT(*) FROM circulation_transaction) AS circulation_rows,
            (SELECT COUNT(*) FROM ottawa_branch_edi_priority) AS ottawa_edi_rows,
            (SELECT COUNT(*) FROM collection_item WHERE accessibility_format IS NOT NULL) AS accessibility_items;
        """
    )


def get_shell_summary() -> pd.DataFrame:
    """Return compact app metadata for the shared header and status bar."""
    return run_query(
        """
        SELECT
            (SELECT COUNT(DISTINCT system_name) FROM library WHERE system_name IS NOT NULL) AS systems,
            (SELECT COUNT(*) FROM library WHERE system_name IS NOT NULL) AS libraries,
            (SELECT COUNT(*) FROM branch_kpi) AS branch_kpi_rows,
            (SELECT MIN(year) FROM branch_kpi) AS min_kpi_year,
            (SELECT MAX(year) FROM branch_kpi) AS max_kpi_year;
        """
    )


def get_searchable_branches() -> pd.DataFrame:
    """Return branch names for the global search bar."""
    return run_query(
        """
        SELECT
            name,
            COALESCE(system_name, 'Unassigned') AS system_name,
            city
        FROM library
        WHERE name IS NOT NULL
        ORDER BY name;
        """
    )


def get_library_locations(filters: DashboardFilters) -> pd.DataFrame:
    """
    Return mappable library rows with coordinates.

    The interactive map depends on real stored coordinates, so I filter out rows
    where latitude or longitude is missing instead of trying to geocode on the
    fly.
    """
    where_sql, params = _library_where(filters)
    return run_query(
        f"""
        WITH branch_metrics AS (
            SELECT
                bk.library_id,
                SUM(COALESCE(bk.circulation, 0)) AS total_circulation,
                SUM(COALESCE(bk.visits, 0)) AS total_visits,
                SUM(COALESCE(bk.registrations, 0)) AS total_registrations
            FROM branch_kpi bk
            GROUP BY bk.library_id
        )
        SELECT
            l.name,
            COALESCE(l.system_name, 'Unassigned') AS system_name,
            l.city,
            l.address,
            l.latitude,
            l.longitude,
            l.ward_name,
            l.neighbourhood_name,
            COALESCE(m.total_circulation, 0) AS total_circulation,
            COALESCE(m.total_visits, 0) AS total_visits,
            COALESCE(m.total_registrations, 0) AS total_registrations
        FROM library l
        LEFT JOIN branch_metrics m ON l.library_id = m.library_id
        {where_sql}
        {"AND" if where_sql else "WHERE"} l.latitude IS NOT NULL
          AND l.longitude IS NOT NULL
        ORDER BY COALESCE(l.system_name, 'Unassigned'), l.name;
        """,
        params,
    )


def get_library_location_coverage(filters: DashboardFilters) -> pd.DataFrame:
    """
    Return mapped versus missing counts for library rows in the current scope.

    I use this on the map page so the coverage note can show real completeness
    by system instead of only describing the visible mapped points.
    """
    where_sql, params = _library_where(filters)
    return run_query(
        f"""
        SELECT
            COALESCE(l.system_name, 'Unassigned') AS system_name,
            COUNT(*) AS total_libraries,
            COUNT(*) FILTER (WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL) AS mapped_libraries,
            COUNT(*) FILTER (WHERE l.latitude IS NULL OR l.longitude IS NULL) AS missing_libraries
        FROM library l
        {where_sql}
        GROUP BY COALESCE(l.system_name, 'Unassigned')
        ORDER BY system_name;
        """,
        params,
    )


def get_system_coverage() -> pd.DataFrame:
    """Return one row per library system with real operational coverage."""
    return run_query(
        """
        WITH kpi_summary AS (
            SELECT
                l.system_name,
                COUNT(DISTINCT l.library_id) AS libraries,
                COUNT(DISTINCT bk.year) AS kpi_years,
                MIN(bk.year) AS min_year,
                MAX(bk.year) AS max_year,
                SUM(COALESCE(bk.circulation, 0)) AS total_circulation,
                SUM(COALESCE(bk.visits, 0)) AS total_visits,
                SUM(COALESCE(bk.registrations, 0)) AS total_registrations
            FROM library l
            LEFT JOIN branch_kpi bk ON l.library_id = bk.library_id
            WHERE l.system_name IS NOT NULL
            GROUP BY l.system_name
        ),
        collection_summary AS (
            SELECT
                COALESCE(l.system_name, 'Unassigned') AS system_name,
                COUNT(*) AS collection_items,
                COUNT(*) FILTER (WHERE ci.publication_year IS NOT NULL) AS items_with_year,
                COUNT(*) FILTER (WHERE ci.accessibility_format IS NOT NULL) AS items_with_accessibility,
                COUNT(DISTINCT ci.format) AS distinct_formats
            FROM collection_item ci
            LEFT JOIN library l ON ci.library_id = l.library_id
            GROUP BY COALESCE(l.system_name, 'Unassigned')
        )
        SELECT
            ks.system_name,
            ks.libraries,
            ks.kpi_years,
            ks.min_year,
            ks.max_year,
            ks.total_circulation,
            ks.total_visits,
            ks.total_registrations,
            COALESCE(cs.collection_items, 0) AS collection_items,
            COALESCE(cs.items_with_year, 0) AS items_with_year,
            COALESCE(cs.items_with_accessibility, 0) AS items_with_accessibility,
            COALESCE(cs.distinct_formats, 0) AS distinct_formats
        FROM kpi_summary ks
        LEFT JOIN collection_summary cs ON ks.system_name = cs.system_name
        ORDER BY ks.libraries DESC;
        """
    )


def get_system_comparison_chart() -> pd.DataFrame:
    """Return compact data for a top-level system comparison chart."""
    return run_query(
        """
        SELECT
            l.system_name AS system_name,
            SUM(COALESCE(bk.circulation, 0)) AS total_circulation
        FROM library l
        JOIN branch_kpi bk ON l.library_id = bk.library_id
        WHERE l.system_name IS NOT NULL
        GROUP BY l.system_name
        ORDER BY total_circulation DESC;
        """
    )


def get_kpi_snapshot(filters: DashboardFilters) -> pd.DataFrame:
    """Return the current KPI snapshot for the selected filters."""
    where_sql, params = _library_where(filters)
    params["year_start"] = filters.year_start
    params["year_end"] = filters.year_end
    return run_query(
        f"""
        SELECT
            COUNT(DISTINCT bk.library_id) AS branches,
            SUM(COALESCE(bk.circulation, 0)) AS total_circulation,
            SUM(COALESCE(bk.visits, 0)) AS total_visits,
            SUM(COALESCE(bk.registrations, 0)) AS total_registrations
        FROM library l
        LEFT JOIN branch_kpi bk
            ON l.library_id = bk.library_id
           AND bk.year BETWEEN :year_start AND :year_end
        {where_sql};
        """,
        params,
    )


def get_circulation_trend(filters: DashboardFilters) -> pd.DataFrame:
    """Return circulation trend data by year."""
    where_sql, params = _library_where(filters)
    params["year_start"] = filters.year_start
    params["year_end"] = filters.year_end
    return run_query(
        f"""
        SELECT
            bk.year,
            SUM(COALESCE(bk.circulation, 0)) AS total_circulation
        FROM library l
        JOIN branch_kpi bk ON l.library_id = bk.library_id
        {where_sql}
        {"AND" if where_sql else "WHERE"} bk.year BETWEEN :year_start AND :year_end
        GROUP BY bk.year
        ORDER BY bk.year;
        """,
        params,
    )


def get_top_branches_by_metric(filters: DashboardFilters, metric: str, limit: int = 15) -> pd.DataFrame:
    """
    Return top branches for a selected KPI metric.

    The metric is constrained in the page logic so this SQL stays safe.
    """
    where_sql, params = _library_where(filters)
    params["year_start"] = filters.year_start
    params["year_end"] = filters.year_end
    params["limit_value"] = limit
    return run_query(
        f"""
        SELECT
            l.name AS branch,
            COALESCE(l.system_name, 'Unassigned') AS system_name,
            SUM(COALESCE(bk.{metric}, 0)) AS metric_value
        FROM library l
        JOIN branch_kpi bk ON l.library_id = bk.library_id
        {where_sql}
        {"AND" if where_sql else "WHERE"} bk.year BETWEEN :year_start AND :year_end
        GROUP BY l.name, COALESCE(l.system_name, 'Unassigned')
        ORDER BY metric_value DESC
        LIMIT :limit_value;
        """,
        params,
    )


def get_metric_breakdown(filters: DashboardFilters) -> pd.DataFrame:
    """Return a long-format dataset for grouped KPI comparisons."""
    where_sql, params = _library_where(filters)
    params["year_start"] = filters.year_start
    params["year_end"] = filters.year_end
    return run_query(
        f"""
        SELECT
            COALESCE(src.system_name, 'Unassigned') AS system_name,
            metric_name,
            metric_value
        FROM (
            SELECT
                l.system_name,
                SUM(COALESCE(bk.circulation, 0)) AS circulation,
                SUM(COALESCE(bk.visits, 0)) AS visits,
                SUM(COALESCE(bk.registrations, 0)) AS registrations
            FROM library l
            JOIN branch_kpi bk ON l.library_id = bk.library_id
            {where_sql}
            {"AND" if where_sql else "WHERE"} bk.year BETWEEN :year_start AND :year_end
            GROUP BY l.system_name
        ) src
        CROSS JOIN LATERAL (
            VALUES
                ('Circulation', circulation),
                ('Visits', visits),
                ('Registrations', registrations)
        ) AS metric(metric_name, metric_value)
        ORDER BY system_name, metric_name;
        """,
        params,
    )


def get_collection_format_distribution(filters: DashboardFilters) -> pd.DataFrame:
    """Return collection format counts for the selected system or branch."""
    where_sql, params = _library_where(filters)
    return run_query(
        f"""
        SELECT
            COALESCE(ci.format, 'Unknown') AS format,
            COUNT(*) AS item_count
        FROM collection_item ci
        LEFT JOIN library l ON ci.library_id = l.library_id
        {where_sql}
        GROUP BY COALESCE(ci.format, 'Unknown')
        ORDER BY item_count DESC
        LIMIT 20;
        """,
        params,
    )


def get_accessibility_distribution(filters: DashboardFilters) -> pd.DataFrame:
    """Return accessibility format counts for the selected filters."""
    where_sql, params = _library_where(filters)
    return run_query(
        f"""
        SELECT
            ci.accessibility_format,
            COUNT(*) AS item_count
        FROM collection_item ci
        LEFT JOIN library l ON ci.library_id = l.library_id
        {where_sql}
        {"AND" if where_sql else "WHERE"} ci.accessibility_format IS NOT NULL
        GROUP BY ci.accessibility_format
        ORDER BY item_count DESC;
        """,
        params,
    )


def get_publication_year_distribution(filters: DashboardFilters) -> pd.DataFrame:
    """Return publication year coverage for the selected filters."""
    where_sql, params = _library_where(filters)
    return run_query(
        f"""
        SELECT
            ci.publication_year,
            COUNT(*) AS item_count
        FROM collection_item ci
        LEFT JOIN library l ON ci.library_id = l.library_id
        {where_sql}
        {"AND" if where_sql else "WHERE"} ci.publication_year IS NOT NULL
        GROUP BY ci.publication_year
        ORDER BY ci.publication_year;
        """,
        params,
    )


def get_ottawa_edi_priority(filters: DashboardFilters) -> pd.DataFrame:
    """Return Ottawa branch EDI priority rows, optionally filtered by branch."""
    params: dict[str, object] = {}
    branch_clause = ""
    if not filters.is_all_branches:
        branch_clause = "WHERE branch_name = :branch_name"
        params["branch_name"] = filters.branch
    return run_query(
        f"""
        SELECT
            branch_name,
            ward_name,
            core_housing_need_pct,
            age_0_14,
            age_65_plus,
            immigrants,
            edi_priority_score
        FROM ottawa_branch_edi_priority
        {branch_clause}
        ORDER BY edi_priority_score DESC;
        """,
        params,
    )


def get_toronto_neighbourhood_context(filters: DashboardFilters) -> pd.DataFrame:
    """Return Toronto branch-to-neighbourhood context rows."""
    params: dict[str, object] = {}
    branch_clause = ""
    if not filters.is_all_branches:
        branch_clause = "AND l.name = :branch_name"
        params["branch_name"] = filters.branch

    return run_query(
        f"""
        SELECT
            l.name AS branch_name,
            l.neighbourhood_no,
            COALESCE(t.neighbourhood_name, l.neighbourhood_name) AS neighbourhood_name,
            t.tsns_designation,
            t.median_after_tax_income_2020,
            t.low_income_lim_at_pct,
            t.low_income_lico_at_pct,
            t.core_housing_need_pct,
            t.shelter_cost_30_plus_pct,
            t.age_0_14_pct,
            t.age_65_plus_pct,
            t.non_official_languages_count,
            t.one_parent_families_count,
            t.one_person_households_count
        FROM library l
        JOIN tpl_neighbourhood_profile t
          ON l.neighbourhood_no = t.neighbourhood_no
        WHERE COALESCE(l.system_name, 'Unassigned') = 'TPL'
          {branch_clause}
        ORDER BY
            t.low_income_lim_at_pct DESC NULLS LAST,
            t.core_housing_need_pct DESC NULLS LAST,
            l.name;
        """,
        params,
    )


def get_branch_profile(branch_name: str, system_name: str | None = None) -> pd.DataFrame:
    """
    Return profile information for a single branch.

    I allow an optional system filter here because branch names are not always
    guaranteed to be globally unique across every city or source system.
    """
    params: dict[str, object] = {"branch_name": branch_name}
    system_clause = ""
    if system_name and system_name != "All Systems":
        system_clause = "AND COALESCE(l.system_name, 'Unassigned') = :system_name"
        params["system_name"] = system_name
    return run_query(
        f"""
        SELECT
            l.name,
            COALESCE(l.system_name, 'Unassigned') AS system_name,
            l.address,
            l.city,
            l.branch_code
        FROM library l
        WHERE l.name = :branch_name
        {system_clause}
        LIMIT 1;
        """,
        params,
    )


def get_branch_kpi_trend(
    branch_name: str,
    year_start: int,
    year_end: int,
    system_name: str | None = None,
) -> pd.DataFrame:
    """Return branch KPI trend data over time."""
    params: dict[str, object] = {
        "branch_name": branch_name,
        "year_start": year_start,
        "year_end": year_end,
    }
    system_clause = ""
    if system_name and system_name != "All Systems":
        system_clause = "AND COALESCE(l.system_name, 'Unassigned') = :system_name"
        params["system_name"] = system_name
    return run_query(
        f"""
        SELECT
            bk.year,
            COALESCE(bk.circulation, 0) AS circulation,
            COALESCE(bk.visits, 0) AS visits,
            COALESCE(bk.registrations, 0) AS registrations
        FROM branch_kpi bk
        JOIN library l ON bk.library_id = l.library_id
        WHERE l.name = :branch_name
          {system_clause}
          AND bk.year BETWEEN :year_start AND :year_end
        ORDER BY bk.year;
        """,
        params,
    )


def get_branch_benchmark(
    branch_name: str,
    year_start: int,
    year_end: int,
    system_name: str | None = None,
) -> pd.DataFrame:
    """Compare a branch to the average branch in its system."""
    params: dict[str, object] = {
        "branch_name": branch_name,
        "year_start": year_start,
        "year_end": year_end,
    }
    target_system_clause = ""
    if system_name and system_name != "All Systems":
        target_system_clause = "AND system_name = :system_name"
        params["system_name"] = system_name
    return run_query(
        f"""
        WITH branch_metrics AS (
            SELECT
                l.name AS branch_name,
                COALESCE(l.system_name, 'Unassigned') AS system_name,
                SUM(COALESCE(bk.circulation, 0)) AS circulation,
                SUM(COALESCE(bk.visits, 0)) AS visits,
                SUM(COALESCE(bk.registrations, 0)) AS registrations
            FROM library l
            JOIN branch_kpi bk ON l.library_id = bk.library_id
            WHERE bk.year BETWEEN :year_start AND :year_end
            GROUP BY l.name, COALESCE(l.system_name, 'Unassigned')
        ),
        target AS (
            SELECT * FROM branch_metrics WHERE branch_name = :branch_name {target_system_clause}
        ),
        system_avg AS (
            SELECT
                system_name,
                AVG(circulation) AS avg_circulation,
                AVG(visits) AS avg_visits,
                AVG(registrations) AS avg_registrations
            FROM branch_metrics
            GROUP BY system_name
        )
        SELECT
            t.branch_name,
            t.system_name,
            t.circulation,
            t.visits,
            t.registrations,
            s.avg_circulation,
            s.avg_visits,
            s.avg_registrations
        FROM target t
        JOIN system_avg s ON t.system_name = s.system_name;
        """,
        params,
    )


def get_data_quality_overview() -> pd.DataFrame:
    """Return a one-row data quality overview for the dashboard."""
    return run_query(
        """
        SELECT
            (SELECT COUNT(*) FROM collection_item) AS collection_items,
            (SELECT COUNT(*) FROM collection_item WHERE publication_year IS NOT NULL) AS items_with_publication_year,
            (SELECT COUNT(*) FROM collection_item WHERE accessibility_format IS NOT NULL) AS items_with_accessibility,
            (SELECT COUNT(*) FROM subject) AS subjects,
            (SELECT COUNT(*) FROM collection_item_subject) AS item_subject_links,
            (SELECT COUNT(*) FROM tpl_neighbourhood_profile) AS toronto_edi_rows,
            (SELECT COUNT(*) FROM ottawa_branch_edi_priority) AS ottawa_edi_rows;
        """
    )


def get_table_inventory() -> pd.DataFrame:
    """Return estimated row counts for the public tables."""
    return run_query(
        """
        SELECT
            relname AS table_name,
            n_live_tup AS estimated_rows
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
        ORDER BY estimated_rows DESC;
        """
    )


def get_supported_metrics_notes() -> pd.DataFrame:
    """
    Return a small explanatory dataset describing what is and is not supported.

    I keep this as data so the app can show a structured status table instead of
    burying key limitations in freeform text.
    """
    return pd.DataFrame(
        [
            {"analytics_area": "Branch KPIs", "status": "Available", "notes": "Real branch KPI coverage exists for TPL and Montreal from 2012 onward."},
            {"analytics_area": "Toronto Neighbourhood Context", "status": "Available", "notes": "Real TPL neighbourhood profile rows are available through tpl_neighbourhood_profile and linked to branches."},
            {"analytics_area": "Ottawa EDI", "status": "Available", "notes": "Real Ottawa branch EDI tables and ward census context are loaded."},
            {"analytics_area": "Accessibility", "status": "Available", "notes": "Real accessibility format values exist in the collection data."},
            {"analytics_area": "Publication Year", "status": "Available", "notes": "Real publication year values are populated for a large share of collection items."},
            {"analytics_area": "Subject Analytics", "status": "Not Loaded", "notes": "Subject and collection_item_subject are currently empty, so subject charts are intentionally excluded."},
        ]
    )
