"""
Geocode missing library coordinates for Bibliometrics+.

Why this script exists:
- the Streamlit map page depends on real latitude/longitude values
- Toronto already has most coordinates loaded
- Ottawa and Montreal still have many missing coordinate values
- the `library` table already stores real branch names and addresses, so we can
  backfill coordinates from those real addresses instead of inventing anything

Important notes:
- this script only updates rows that are currently missing coordinates
- it only targets Ottawa (`OPL`) and Montreal because those are the systems
  that still need help for the interactive map
- it uses the public Nominatim geocoding API politely with a custom user agent
  and a pause between requests
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import requests
from sqlalchemy import text

from db import engine


USER_AGENT = "BibliometricsPlusGeocoder/1.0 (Capstone Project Dashboard)"
GEOCODER_URL = "https://nominatim.openstreetmap.org/search"
TARGET_SYSTEMS = ("OPL", "Montreal")
REQUEST_DELAY_SECONDS = 1.1


@dataclass
class LibraryRow:
    """Represents one library record that needs coordinates."""

    library_id: int
    name: str
    address: str
    city: str
    system_name: str


def fetch_missing_libraries() -> list[LibraryRow]:
    """
    Read library rows that still need coordinates.

    I only include rows with a non-empty address because geocoding without an
    address would add noise and bad results to the final map.
    """
    sql = """
        SELECT
            library_id,
            name,
            address,
            city,
            system_name
        FROM library
        WHERE system_name IN ('OPL', 'Montreal')
          AND (latitude IS NULL OR longitude IS NULL)
          AND address IS NOT NULL
          AND address <> ''
        ORDER BY system_name, name;
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    return [
        LibraryRow(
            library_id=row.library_id,
            name=row.name,
            address=row.address,
            city=row.city,
            system_name=row.system_name,
        )
        for row in rows
    ]


def build_query_candidates(row: LibraryRow) -> list[str]:
    """
    Create a few address variants for geocoding.

    Real-world address data is messy, so I try the most specific version first
    and then a couple of simpler fallbacks if needed.
    """
    base_city = row.city or ("Ottawa" if row.system_name == "OPL" else "Montréal")
    country = "Canada"

    return [
        f"{row.address}, {base_city}, Ontario, {country}" if row.system_name == "OPL" else f"{row.address}, {base_city}, Quebec, {country}",
        f"{row.address}, {base_city}, {country}",
        f"{row.name}, {row.address}, {base_city}, {country}",
    ]


def geocode_one(query: str) -> tuple[float, float] | None:
    """
    Send one request to the geocoder and return coordinates if we get a match.
    """
    response = requests.get(
        GEOCODER_URL,
        params={
            "q": query,
            "format": "jsonv2",
            "limit": 1,
        },
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    if not payload:
        return None

    first_match = payload[0]
    return float(first_match["lat"]), float(first_match["lon"])


def geocode_library(row: LibraryRow) -> tuple[float, float] | None:
    """
    Try a few query variants until one returns coordinates.
    """
    for query in build_query_candidates(row):
        try:
            result = geocode_one(query)
            if result is not None:
                return result
        except requests.RequestException as exc:
            print(f"[WARN] Geocoding request failed for {row.name}: {exc}")
            return None
        finally:
            time.sleep(REQUEST_DELAY_SECONDS)
    return None


def update_coordinates(library_id: int, latitude: float, longitude: float) -> None:
    """Write the geocoded coordinates back into the live database."""
    sql = """
        UPDATE library
        SET latitude = :latitude,
            longitude = :longitude
        WHERE library_id = :library_id;
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "library_id": library_id,
                "latitude": latitude,
                "longitude": longitude,
            },
        )


def main() -> None:
    """
    Backfill missing coordinates and print a small summary.
    """
    libraries = fetch_missing_libraries()
    print(f"Found {len(libraries)} libraries with missing coordinates and usable addresses.")

    updated = 0
    skipped = 0

    for row in libraries:
        print(f"[LOOKUP] {row.system_name} | {row.name} | {row.address}, {row.city}")
        result = geocode_library(row)

        if result is None:
            skipped += 1
            print(f"[SKIP ] No coordinate match found for {row.name}.")
            continue

        latitude, longitude = result
        update_coordinates(row.library_id, latitude, longitude)
        updated += 1
        print(f"[WRITE] Updated {row.name} -> ({latitude:.6f}, {longitude:.6f})")

    print()
    print("Coordinate backfill complete.")
    print(f"Updated rows: {updated}")
    print(f"Skipped rows: {skipped}")


if __name__ == "__main__":
    main()
