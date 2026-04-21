"""
Library access and branch discovery page.

I rebuilt this page to feel more aligned with the project instead of acting
like a generic pin map. The goal here is to help someone search a place,
discover nearby libraries, and still connect that search back to real branch
analytics and EDI context where available.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

# Streamlit executes page files directly, so I add the repo root before
# importing shared `app.*` modules.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.charts import grouped_bar_chart, line_chart
from app.components.layout import (
    configure_page,
    render_app_shell,
    render_chart_guide,
    render_chart_summary,
    render_filter_summary,
    render_footer,
    render_page_header,
    render_section_intro,
)
from app.components.tables import show_dataframe
from app.services.chart_insights import summarize_grouped_bars, summarize_time_series
from app.services.filters import DashboardFilters, render_global_filters
from app.services.formatters import format_distance_km, format_int
from app.services.queries import (
    get_branch_benchmark,
    get_branch_kpi_trend,
    get_branch_profile,
    get_library_location_coverage,
    get_library_locations,
    get_ottawa_edi_priority,
)
from app.styles.theme import apply_theme, get_theme_tokens


GEOCODER_URL = "https://nominatim.openstreetmap.org/search"
GEOCODER_USER_AGENT = "BibliometricsPlusMapSearch/1.0"
SYSTEM_COLORS = {
    "TPL": [28, 126, 214, 185],
    "OPL": [35, 163, 106, 185],
    "Montreal": [21, 48, 60, 185],
    "Unassigned": [120, 120, 120, 180],
}


@st.cache_data(show_spinner=False, ttl=60 * 60 * 24)
def geocode_search_location(query: str) -> tuple[float, float, str] | None:
    """
    Convert a user-entered place into coordinates.

    The page stays real-data driven because the library points come from the
    database, while this lightweight geocoding step only helps center the map
    around the user's searched place.
    """
    response = requests.get(
        GEOCODER_URL,
        params={"q": query, "format": "jsonv2", "limit": 1},
        headers={"User-Agent": GEOCODER_USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        return None

    match = payload[0]
    return float(match["lat"]), float(match["lon"]), match.get("display_name", query)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the distance between two points in kilometers."""
    radius_km = 6371.0
    lat1_rad, lon1_rad = math.radians(lat1), math.radians(lon1)
    lat2_rad, lon2_rad = math.radians(lat2), math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_km * c


def build_locator_map(
    library_df: pd.DataFrame,
    search_result: tuple[float, float, str] | None,
    system_scope: str,
) -> pdk.Deck:
    """Build the interactive map deck for library discovery."""
    theme = get_theme_tokens()
    if search_result is not None:
        view_state = pdk.ViewState(
            latitude=search_result[0],
            longitude=search_result[1],
            zoom=10.8 if system_scope == "All Systems" else 11.2,
            pitch=0,
        )
    else:
        view_state = pdk.ViewState(
            latitude=float(library_df["latitude"].mean()),
            longitude=float(library_df["longitude"].mean()),
            zoom=8.1 if system_scope == "All Systems" else 10.4,
            pitch=0,
        )

    library_layer = pdk.Layer(
        "ScatterplotLayer",
        data=library_df,
        get_position="[longitude, latitude]",
        get_fill_color="color",
        get_line_color=[255, 255, 255, 180],
        get_radius="radius",
        pickable=True,
        stroked=True,
        line_width_min_pixels=1,
    )

    layers = [library_layer]
    if search_result is not None:
        search_df = pd.DataFrame(
            [
                {
                    "name": "Searched location",
                    "system_name": "Search point",
                    "city": search_result[2],
                    "address": search_result[2],
                    "latitude": search_result[0],
                    "longitude": search_result[1],
                    "distance_label": "0.0 km",
                    "total_circulation": 0,
                    "total_visits": 0,
                }
            ]
        )
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=search_df,
                get_position="[longitude, latitude]",
                get_fill_color=[232, 93, 79, 220],
                get_line_color=[255, 255, 255, 220],
                get_radius=1450,
                pickable=True,
                stroked=True,
                line_width_min_pixels=2,
            )
        )

    tooltip = {
        "html": """
            <div style="font-family: sans-serif;">
                <div><strong>{name}</strong></div>
                <div>{system_name} | {city}</div>
                <div>{address}</div>
                <div>Distance: {distance_label}</div>
                <div>Circulation: {total_circulation}</div>
                <div>Visits: {total_visits}</div>
                <div>Ward: {ward_name}</div>
                <div>Neighbourhood: {neighbourhood_name}</div>
            </div>
        """,
        "style": {
            "backgroundColor": theme["shell"],
            "color": "white",
            "border": f"1px solid {theme['secondary']}",
        },
    }

    return pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v10",
        initial_view_state=view_state,
        layers=layers,
        tooltip=tooltip,
    )


def render_nearest_library_cards(nearest_df: pd.DataFrame) -> None:
    """Render a small set of ranked nearby branch cards."""
    theme = get_theme_tokens()
    for index, row in enumerate(nearest_df.head(5).to_dict(orient="records"), start=1):
        st.markdown(
            f"""
            <div class="bm-map-panel">
                <div class="bm-topbar-label">Nearest Match {index}</div>
                <div class="bm-topbar-value" style="font-size:1.02rem;">{row['name']}</div>
                <div style="margin-top:0.45rem; color:{theme['muted']};">
                    {row['system_name']} | {row['city']} | {row['distance_label']}
                </div>
                <div style="margin-top:0.35rem;">{row['address'] or 'Address unavailable in source records'}</div>
                <div style="margin-top:0.55rem; color:{theme['muted']}; font-size:0.92rem;">
                    Circulation: {format_int(row['total_circulation'])} | Visits: {format_int(row['total_visits'])}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


configure_page("Bibliometrics+ | Library Access")
apply_theme()
filters = render_global_filters()
render_app_shell("Library Access")

render_page_header(
    "Library Access & Branch Discovery",
    "Search a location, find nearby libraries across Toronto, Ottawa, and Montreal, and connect those branches back to real KPI and EDI context.",
)
render_filter_summary(
    [
        f"System: {filters.system}",
        f"Branch filter scope: {filters.branch}",
        "This page combines location search, branch discovery, and real branch analytics in one place.",
    ]
)

locations_df = get_library_locations(filters)
coverage_df = get_library_location_coverage(filters)

if locations_df.empty:
    st.warning("No mapped library locations are available for the current filter selection.")
    render_footer()
    st.stop()

locations_df = locations_df.copy()
locations_df["color"] = locations_df["system_name"].map(SYSTEM_COLORS).apply(
    lambda value: value if isinstance(value, list) else SYSTEM_COLORS["Unassigned"]
)
locations_df["radius"] = locations_df["system_name"].map({"TPL": 950, "OPL": 1050, "Montreal": 1000}).fillna(900)

coverage_totals = coverage_df[["total_libraries", "mapped_libraries", "missing_libraries"]].sum()

st.markdown(
    """
    <div class="bm-search-card">
        <h3>Find a Nearby Library</h3>
        <p>Search a neighbourhood, address, campus, or postal code and the application will rank the nearest mapped libraries using stored branch coordinates.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

default_search = {
    "All Systems": "Downtown Toronto, Ontario",
    "OPL": "Downtown Ottawa, Ontario",
    "TPL": "Downtown Toronto, Ontario",
    "Montreal": "Downtown Montréal, Québec",
}.get(filters.system, "Downtown Toronto, Ontario")

search_result = None
search_error = None
with st.form("library_locator_search_form", border=False):
    search_location = st.text_input(
        "Search a location",
        value=st.session_state.get("library_map_search", default_search),
        key="library_map_search_input",
        help="Examples: Downtown Ottawa, University of Toronto, Scarborough Town Centre, Plateau-Mont-Royal",
    )
    submitted = st.form_submit_button("Find nearby libraries", width="stretch")

if submitted:
    st.session_state["library_map_search"] = search_location.strip()

active_search = st.session_state.get("library_map_search", default_search).strip()
if active_search:
    try:
        search_result = geocode_search_location(active_search)
        if search_result is None:
            search_error = "No matching location was found. Try a more specific address, postal code, or neighbourhood."
    except requests.RequestException as exc:
        search_error = f"Location search failed: {exc}"

if search_error:
    st.warning(search_error)

if search_result is not None:
    search_lat, search_lon, search_label = search_result
    locations_df["distance_km"] = locations_df.apply(
        lambda row: haversine_km(search_lat, search_lon, float(row["latitude"]), float(row["longitude"])),
        axis=1,
    )
    locations_df = locations_df.sort_values("distance_km", ascending=True).reset_index(drop=True)
else:
    search_label = None
    locations_df["distance_km"] = None

locations_df["distance_label"] = locations_df["distance_km"].apply(format_distance_km)

metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
with metric_col_1:
    st.metric("Mapped Libraries", format_int(coverage_totals["mapped_libraries"]))
with metric_col_2:
    st.metric("Systems Visible", format_int(locations_df["system_name"].nunique()))
with metric_col_3:
    st.metric("Missing Coordinates", format_int(coverage_totals["missing_libraries"]))

if int(coverage_totals["missing_libraries"]) > 0:
    st.info(
        "Coverage note: this locator uses only real saved coordinates. "
        f"In the current scope, {format_int(coverage_totals['mapped_libraries'])} libraries are mapped and "
        f"{format_int(coverage_totals['missing_libraries'])} still do not have stored coordinates."
    )

legend_html = """
<div class="bm-map-panel">
    <div class="bm-map-legend">
        <span class="bm-legend-item"><span class="bm-legend-dot" style="background:#1C7ED6;"></span>Toronto Public Library</span>
        <span class="bm-legend-item"><span class="bm-legend-dot" style="background:#23A36A;"></span>Ottawa Public Library</span>
        <span class="bm-legend-item"><span class="bm-legend-dot" style="background:#15303C;"></span>Montréal Libraries</span>
        <span class="bm-legend-item"><span class="bm-legend-dot" style="background:#E85D4F;"></span>Searched Location</span>
    </div>
</div>
"""

map_col, sidebar_col = st.columns((1.35, 0.95))
with map_col:
    render_section_intro(
        "Interactive Library Locator",
        "This visualization is centered around discovery rather than just plotting points. Search a place, inspect the branch mix nearby, and use the results to understand access across the three library systems.",
    )
    st.markdown(legend_html, unsafe_allow_html=True)
    st.pydeck_chart(
        build_locator_map(locations_df, search_result, filters.system),
        width="stretch",
    )
    render_chart_guide("Point colors identify library systems, and the red point marks the searched location. The nearest result cards on the right explain what the map is showing.")
    if search_result is not None and not locations_df.empty:
        nearest_row = locations_df.iloc[0]
        render_chart_summary(
            f"The nearest mapped branch to {search_label} is {nearest_row['name']} in {nearest_row['system_name']} at {nearest_row['distance_label']}."
        )
    else:
        render_chart_summary(
            f"This map currently shows {format_int(locations_df['system_name'].nunique())} library systems and {format_int(len(locations_df))} mapped branches in scope."
        )

with sidebar_col:
    render_section_intro(
        "Nearby Library Results",
        "Nearby libraries are ranked first to support access and service discovery within the selected geographic area.",
    )
    if search_result is not None:
        st.caption(f"Search result: {search_label}")
        render_nearest_library_cards(locations_df)
    else:
        st.info("Search a location above to rank the nearest libraries.")

    render_section_intro(
        "Map Coverage by System",
        "This table shows how complete the stored coordinate coverage is for each system in the current filter scope.",
    )
    show_dataframe(coverage_df)

render_section_intro(
    "Branch Insight Panel",
    "This section lets the locator stay connected to the rest of the project by showing real branch KPI trends and EDI context for the branch you want to inspect.",
)

if search_result is not None:
    selector_source_df = locations_df.head(12).copy()
else:
    selector_source_df = locations_df.sort_values(["system_name", "name"]).head(50).copy()

branch_labels = [
    f"{row['name']} ({row['system_name']})"
    for row in selector_source_df.to_dict(orient="records")
]
branch_lookup = dict(
    zip(
        branch_labels,
        selector_source_df[["name", "system_name"]].to_dict(orient="records"),
        strict=False,
    )
)

default_branch_label = branch_labels[0] if branch_labels else None
if not branch_labels:
    st.info("No branch options are available for the current locator scope.")
    selected_branch_label = None
else:
    selected_branch_label = st.selectbox(
        "Choose a branch to inspect on this page",
        options=branch_labels,
        index=0,
    )

if selected_branch_label:
    selected_branch = branch_lookup[selected_branch_label]
    branch_name = selected_branch["name"]
    branch_system = selected_branch["system_name"]

    profile_df = get_branch_profile(branch_name, branch_system)
    trend_df = get_branch_kpi_trend(branch_name, filters.year_start, filters.year_end, branch_system)
    benchmark_df = get_branch_benchmark(branch_name, filters.year_start, filters.year_end, branch_system)

    if not profile_df.empty:
        profile = profile_df.iloc[0]
        summary_cols = st.columns(4)
        with summary_cols[0]:
            st.metric("Selected Branch", profile["name"])
        with summary_cols[1]:
            st.metric("System", profile["system_name"])
        with summary_cols[2]:
            st.metric("City", profile["city"] or "Unknown")
        with summary_cols[3]:
            st.metric("Branch Code", profile["branch_code"] or "N/A")
        st.caption(f"Address: {profile['address'] or 'Address unavailable in source records.'}")

    chart_left, chart_right = st.columns(2)
    with chart_left:
        render_section_intro(
            "Branch KPI Trend",
            "This keeps the locator tied to the analytics side of the project by showing how the selected branch changes over time across the chosen years.",
        )
        if trend_df.empty:
            st.info("No KPI trend data is available for this branch in the current year range.")
        else:
            st.altair_chart(
                line_chart(
                    trend_df,
                    x="year:O",
                    y="circulation:Q",
                    tooltip=["year", "circulation", "visits", "registrations"],
                    height=320,
                ),
                width="stretch",
            )
            render_chart_guide("The line tracks the selected branch's circulation over time, so higher points mean stronger circulation years.")
            render_chart_summary(
                summarize_time_series(
                    trend_df,
                    x_col="year",
                    y_col="circulation",
                    metric_label="Branch circulation",
                )
            )

    with chart_right:
        render_section_intro(
            "Branch vs System Average",
            "This helps explain whether the selected branch is operating above or below the average branch in its own system.",
        )
        if benchmark_df.empty:
            st.info("No benchmark comparison is available for this branch.")
        else:
            benchmark = benchmark_df.iloc[0]
            compare_cols = st.columns(3)
            with compare_cols[0]:
                st.metric(
                    "Circulation",
                    format_int(benchmark["circulation"]),
                    delta=format_int(benchmark["circulation"] - benchmark["avg_circulation"]),
                )
            with compare_cols[1]:
                st.metric(
                    "Visits",
                    format_int(benchmark["visits"]),
                    delta=format_int(benchmark["visits"] - benchmark["avg_visits"]),
                )
            with compare_cols[2]:
                st.metric(
                    "Registrations",
                    format_int(benchmark["registrations"]),
                    delta=format_int(benchmark["registrations"] - benchmark["avg_registrations"]),
                )

    if not trend_df.empty:
        trend_long = pd.melt(
            trend_df,
            id_vars=["year"],
            value_vars=["circulation", "visits", "registrations"],
            var_name="metric_name",
            value_name="metric_value",
        )
        trend_long["metric_name"] = trend_long["metric_name"].str.title()

        render_section_intro(
            "Metric Mix by Year",
            "This grouped view makes the selected branch easier to explain by showing circulation, visits, and registrations together.",
        )
        st.altair_chart(
            grouped_bar_chart(
                trend_long,
                x="year:O",
                y="metric_value:Q",
                color="metric_name:N",
                tooltip=["year", "metric_name", "metric_value"],
                height=320,
            ),
            width="stretch",
        )
        render_chart_guide("Each year groups circulation, visits, and registrations side by side so the branch mix is readable without using a heatmap.")
        render_chart_summary(
            summarize_grouped_bars(
                trend_long,
                group_col="year",
                category_col="metric_name",
                value_col="metric_value",
                value_label="activity",
            )
        )

    if branch_system == "OPL":
        render_section_intro(
            "Ottawa EDI Context",
            "Because this branch belongs to Ottawa, the locator can also surface the real EDI priority row that links the branch to ward-level context indicators.",
        )
        ottawa_filters = DashboardFilters(
            system="OPL",
            branch=branch_name,
            year_start=filters.year_start,
            year_end=filters.year_end,
        )
        ottawa_edi_df = get_ottawa_edi_priority(ottawa_filters)
        if ottawa_edi_df.empty:
            st.info("No Ottawa EDI context row was found for this branch.")
        else:
            show_dataframe(ottawa_edi_df)
    else:
        st.caption("Ottawa-specific EDI context appears only for Ottawa Public Library branches.")

render_section_intro(
    "Mapped Library Table",
    "This full table stays available for transparency and gives you a complete view of the mapped branch rows supporting the locator page.",
)
show_dataframe(
    locations_df[
        [
            "name",
            "system_name",
            "city",
            "distance_label",
            "address",
            "ward_name",
            "neighbourhood_name",
            "total_circulation",
            "total_visits",
            "total_registrations",
            "latitude",
            "longitude",
        ]
    ].head(50)
)

render_footer()
