"""
NYC Hotel Property Tax Scraper
Pulls all building class H (hotel) properties from NYC Open Data
and the DOF Property Assessment dataset, then outputs to hotel-comps.json

Data source: NYC Department of Finance - Final Assessment Roll
API: NYC Open Data (Socrata) - no API key required for small queries
Dataset: Property Valuation and Assessment Data

Building Classes:
  H1 - Luxury Hotel
  H2 - Full Service Hotel
  H3 - Limited Service Hotel (Budget)
  H4 - Motel
  H5 - Hotel (Private Club / Membership)
  H6 - Apartment Hotel
  H7 - (unused)
  H8 - Dormitory
  H9 - Miscellaneous Hotel
  HB - Boutique Hotel
  HH - Hostels
  HR - SRO (Single Room Occupancy)

We want: H1, H2, H3, H4, H5, H6, H9, HB (commercial hotels)
"""

import json
import requests
import sys
import os
from datetime import datetime

# NYC Open Data - Property Assessment dataset (latest final roll)
# This dataset includes ALL NYC properties with their assessments
# Socrata dataset ID for DOF Property Valuation and Assessment Data
DATASET_URL = "https://data.cityofnewyork.us/resource/8y4t-faws.json"

# Hotel building classes we care about (exclude dormitories, SROs, hostels)
HOTEL_CLASSES = ["H1", "H2", "H3", "H4", "H5", "H6", "H9", "HB"]

# Borough codes
BOROUGH_MAP = {
    "1": "Manhattan",
    "2": "Bronx",
    "3": "Brooklyn",
    "4": "Queens",
    "5": "Staten Island"
}

def fetch_hotel_properties():
    """Fetch all hotel-class properties from NYC Open Data"""
    all_hotels = []

    # Use the latest assessment year available
    # First, detect the max year in the dataset
    try:
        resp = requests.get(DATASET_URL, params={
            "$select": "max(year) as max_year",
            "$where": "bldg_class='H1'"
        }, timeout=30)
        resp.raise_for_status()
        max_year = resp.json()[0].get("max_year", "2027")
        print(f"Using assessment year: {max_year}\n")
    except Exception:
        max_year = "2027"
        print(f"Defaulting to assessment year: {max_year}\n")

    for bldg_class in HOTEL_CLASSES:
        print(f"Fetching building class {bldg_class}...")

        params = {
            "$where": f"bldg_class='{bldg_class}' AND year='{max_year}'",
            "$limit": 50000,
        }

        try:
            resp = requests.get(DATASET_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            print(f"  Found {len(data)} properties for class {bldg_class}")

            for row in data:
                hotel = parse_property(row, bldg_class)
                if hotel:
                    all_hotels.append(hotel)

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching {bldg_class}: {e}")

    return all_hotels


def parse_property(row, bldg_class):
    """Parse a Socrata API row into our hotel comp schema"""
    try:
        boro_code = str(row.get("boro", ""))
        borough = BOROUGH_MAP.get(boro_code, boro_code)
        block = str(row.get("block", "")).lstrip("0")
        lot = str(row.get("lot", "")).lstrip("0")
        bbl = f"{boro_code}-{block.zfill(5)}-{lot.zfill(4)}"

        # Address
        house_lo = row.get("housenum_lo", "").strip() if row.get("housenum_lo") else ""
        house_hi = row.get("housenum_hi", "").strip() if row.get("housenum_hi") else ""
        street = row.get("street_name", "").strip() if row.get("street_name") else ""

        if house_hi and house_hi != house_lo:
            address = f"{house_lo}-{house_hi} {street}"
        elif house_lo:
            address = f"{house_lo} {street}"
        else:
            address = street

        # Values — field names from 8y4t-faws dataset
        market_value = safe_int(row.get("curmkttot"))
        taxable_av = safe_int(row.get("curacttot"))
        # curtxbtot = current taxable before exemptions
        annual_tax_before = safe_float(row.get("curtxbtot"))
        tax_exempt = safe_float(row.get("curtxbextot")) or 0
        annual_tax = annual_tax_before - tax_exempt if annual_tax_before else None

        gross_sqft = safe_int(row.get("gross_sqft"))
        hotel_sqft = safe_int(row.get("hotel_area_gross"))
        units = safe_int(row.get("units"))
        stories = safe_int(row.get("bld_story"))
        year_built = safe_int(row.get("yrbuilt"))

        # Building class descriptions
        class_desc = {
            "H1": "Luxury Hotel",
            "H2": "Full Service Hotel",
            "H3": "Limited Service Hotel",
            "H4": "Motel",
            "H5": "Private Club Hotel",
            "H6": "Apartment Hotel",
            "H9": "Miscellaneous Hotel",
            "HB": "Boutique Hotel"
        }

        owner = row.get("owner", "").strip() if row.get("owner") else ""

        return {
            "id": f"COMP-{bbl.replace('-', '')}",
            "hotelName": f"{address}, {borough}",  # Will be enriched with actual hotel names later
            "brand": None,  # From participation report
            "hotelClass": None,  # STR class - from participation report
            "buildingClass": bldg_class,
            "buildingClassDesc": class_desc.get(bldg_class, bldg_class),
            "address": address,
            "neighborhood": None,  # Can be enriched via geocoding or manual
            "borough": borough,
            "zip": row.get("zip_code", "").strip() if row.get("zip_code") else "",
            "bbl": bbl,
            "block": block,
            "lot": lot,
            "keys": units if units and units > 0 else None,  # Units field sometimes has room count
            "stories": stories,
            "yearBuilt": year_built,
            "grossSqft": gross_sqft,
            "hotelSqft": hotel_sqft,
            "owner": owner,
            "lat": None,  # Can be geocoded
            "lng": None,
            "marketValue": market_value,
            "taxableAV": taxable_av,
            "currentTaxRate": None,  # Calculate from tax / taxableAV
            "annualTax": round(annual_tax) if annual_tax else None,
            "annualTaxBeforeAbatements": round(annual_tax_before) if annual_tax_before else None,
            "grossIncome": None,  # Not available in assessment dataset
            "expenses": None,
            "historicalTaxPerKey": [],
            "notes": ""
        }

    except Exception as e:
        print(f"  Error parsing row: {e}")
        return None


def safe_int(val):
    """Safely convert to int"""
    if val is None or val == "":
        return None
    try:
        return int(float(str(val).replace(",", "")))
    except (ValueError, TypeError):
        return None


def safe_float(val):
    """Safely convert to float"""
    if val is None or val == "":
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def calculate_tax_rate(hotel):
    """Calculate effective tax rate"""
    if hotel["annualTax"] and hotel["taxableAV"] and hotel["taxableAV"] > 0:
        hotel["currentTaxRate"] = round(hotel["annualTax"] / hotel["taxableAV"], 5)
    return hotel


def build_output(hotels):
    """Build the hotel-comps.json output"""
    # Calculate tax rates
    hotels = [calculate_tax_rate(h) for h in hotels]

    # Sort by annual tax descending
    hotels.sort(key=lambda h: h.get("annualTax") or 0, reverse=True)

    # Assign sequential IDs
    for i, h in enumerate(hotels, 1):
        h["id"] = f"COMP-{i:04d}"

    output = {
        "lastUpdated": datetime.now().strftime("%Y-%m-%d"),
        "metadata": {
            "description": "NYC Hotels — Property Tax Assessment Data (All H-class properties)",
            "source": "NYC Department of Finance via NYC Open Data API",
            "scrapedAt": datetime.now().isoformat(),
            "criteria": {
                "geography": "New York City (all 5 boroughs)",
                "buildingClasses": HOTEL_CLASSES,
                "dataPoints": ["Market value", "Taxable AV", "Annual tax", "Gross sqft", "Units", "Gross income"]
            },
            "notes": "Key counts need to be enriched from STR participation report. 'units' field may approximate keys for some properties."
        },
        "hotels": hotels
    }

    return output


def main():
    print("=" * 60)
    print("NYC Hotel Property Tax Scraper")
    print("=" * 60)
    print()

    hotels = fetch_hotel_properties()

    print(f"\nTotal hotel properties found: {len(hotels)}")

    # Summary by borough
    by_borough = {}
    for h in hotels:
        b = h["borough"]
        by_borough[b] = by_borough.get(b, 0) + 1
    for b, c in sorted(by_borough.items(), key=lambda x: -x[1]):
        print(f"  {b}: {c}")

    # Summary by building class
    by_class = {}
    for h in hotels:
        c = h["buildingClass"]
        by_class[c] = by_class.get(c, 0) + 1
    print()
    for c, n in sorted(by_class.items()):
        print(f"  {c} ({hotels[0]['buildingClassDesc'] if hotels else c}): {n}")

    # Properties with tax data
    with_tax = [h for h in hotels if h["annualTax"] and h["annualTax"] > 0]
    print(f"\nProperties with tax data: {len(with_tax)}")

    total_tax = sum(h["annualTax"] for h in with_tax)
    print(f"Total annual hotel property tax: ${total_tax:,.0f}")

    # Build and save output
    output = build_output(hotels)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, "hotel-comps.json")

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved to: {output_path}")
    print(f"Hotels in output: {len(output['hotels'])}")
    print("\nNext steps:")
    print("  1. Cross-reference with participation report to add key counts")
    print("  2. Filter to 100+ key properties")
    print("  3. Add hotel names, brands, and STR classes")
    print("  4. Geocode addresses for map markers")
    print("  5. Push to GitHub to update dashboard")


if __name__ == "__main__":
    main()
