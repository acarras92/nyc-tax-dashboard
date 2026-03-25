"""
Merge CoStar hotel data (key counts, brands, names) with DOF tax data (assessments, taxes).
Reads costar_hotels.json + fetches DOF data from NYC Open Data API.
Outputs enriched hotel-comps.json for the dashboard.

Run from Claude Code:
  cd C:/Users/acarr/Documents/Highgate/Tools/nyc-tax-scraper
  pip install requests
  python merge_costar_dof.py
"""

import json
import re
import requests
from datetime import datetime

# NYC Open Data - DOF Property Assessment (latest final roll)
DOF_API = "https://data.cityofnewyork.us/resource/8y4t-faws.json"
HOTEL_CLASSES = ["H1", "H2", "H3", "H4", "H5", "H6", "H9", "HB"]

CLASS_DESC = {
    "H1": "Luxury Hotel", "H2": "Full Service Hotel", "H3": "Limited Service Hotel",
    "H4": "Motel", "H5": "Private Club Hotel", "H6": "Apartment Hotel",
    "H9": "Miscellaneous Hotel", "HB": "Boutique Hotel"
}

HOTEL_CLASS_MAP = {
    "Luxury": "luxury", "Upper Upscale": "upper-upscale", "Upscale": "upscale",
    "Upper Midscale": "upper-midscale", "Midscale": "midscale", "Economy": "economy"
}

SUBMARKET_MAP = {
    "Midtown South": "Midtown South",
    "Midtown West/Times Square": "Midtown West",
    "Midtown East": "Midtown East",
    "Village/Soho/Tribeca": "Downtown",
    "Financial District": "Financial District",
    "Uptown": "Uptown",
}

# Address normalization
STREET_ABBREVS = {
    "STREET": "ST", "AVENUE": "AVE", "BOULEVARD": "BLVD", "DRIVE": "DR",
    "PLACE": "PL", "ROAD": "RD", "LANE": "LN", "COURT": "CT", "SQUARE": "SQ",
    "WEST": "W", "EAST": "E", "NORTH": "N", "SOUTH": "S",
    "1ST": "1", "2ND": "2", "3RD": "3", "4TH": "4", "5TH": "5",
    "6TH": "6", "7TH": "7", "8TH": "8", "9TH": "9", "10TH": "10",
    "11TH": "11", "12TH": "12",
}


def normalize(addr):
    if not addr:
        return ""
    addr = str(addr).upper().strip()
    addr = re.sub(r'\s+(APT|STE|SUITE|UNIT|FL|FLOOR)\s*\S*', '', addr)
    for old, new in STREET_ABBREVS.items():
        addr = re.sub(r'\b' + old + r'\b', new, addr)
    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr


def house_num(addr):
    m = re.match(r'^(\d+)', str(addr).strip())
    return m.group(1) if m else ""


def fetch_dof_hotels():
    """Fetch all H-class properties from NYC Open Data"""
    all_props = []
    for bc in HOTEL_CLASSES:
        print(f"  Fetching DOF class {bc}...")
        try:
            resp = requests.get(DOF_API, params={
                "$where": f"bldg_class='{bc}'",
                "$limit": 10000
            }, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            print(f"    {len(data)} properties")
            for row in data:
                h = str(row.get("housenum_lo", "")).strip()
                s = str(row.get("street_name", "")).strip()
                boro = str(row.get("boro", ""))
                block = str(row.get("block", "")).strip().zfill(5)
                lot = str(row.get("lot", "")).strip().zfill(4)
                all_props.append({
                    "address_raw": f"{h} {s}".strip(),
                    "address_norm": normalize(f"{h} {s}"),
                    "house_num": house_num(h),
                    "boro": boro,
                    "bbl": f"{boro}-{block}-{lot}",
                    "owner": str(row.get("owner", "")).strip(),
                    "bldg_class": bc,
                    "market_value": int(float(row.get("curmkttot", 0) or 0)),
                    "taxable_av": int(float(row.get("curacttot", 0) or 0)),
                    "tax_before": round(float(row.get("curtxbtot", 0) or 0)),
                    "tax_after": round(float(row.get("curtaxatot", 0) or 0)) if row.get("curtaxatot") else None,
                    "gross_sqft": int(float(row.get("gross_sqft", 0) or 0)) if row.get("gross_sqft") else None,
                    "units": int(float(row.get("units", 0) or 0)) if row.get("units") else None,
                    "zip": str(row.get("zip", "")).strip()[:5],
                })
        except Exception as e:
            print(f"    ERROR: {e}")
    return all_props


def match_hotels(costar, dof):
    """Match CoStar records to DOF by address"""
    matched = 0
    results = []

    for i, cs in enumerate(costar):
        cs_norm = normalize(cs["address"])
        cs_num = house_num(cs["address"])
        cs_zip = cs.get("zip", "")[:5]

        best = None
        best_score = 0

        for d in dof:
            # Only match Manhattan (boro 1) since CoStar data is Manhattan
            if d["boro"] != "1":
                continue

            score = 0

            # House number match
            if cs_num and d["house_num"] and cs_num == d["house_num"]:
                score += 10

                # Street word overlap
                cs_street = cs_norm.replace(cs_num, "").strip()
                d_street = d["address_norm"].replace(d["house_num"], "").strip()
                cs_words = set(cs_street.split())
                d_words = set(d_street.split())
                overlap = cs_words & d_words
                score += len(overlap) * 3

                # Zip match bonus
                if cs_zip and d["zip"] and cs_zip == d["zip"]:
                    score += 5

            if score > best_score:
                best_score = score
                best = d

        record = {
            "id": f"COMP-{i+1:04d}",
            "hotelName": cs["hotelName"],
            "brand": cs.get("brand"),
            "parentCompany": cs.get("parentCompany"),
            "hotelClass": HOTEL_CLASS_MAP.get(cs.get("hotelClass", ""), None),
            "hotelClassDisplay": cs.get("hotelClass"),
            "scale": cs.get("scale"),
            "operationType": cs.get("operationType"),
            "buildingClass": None,
            "buildingClassDesc": None,
            "address": cs["address"],
            "neighborhood": SUBMARKET_MAP.get(cs.get("submarket", ""), cs.get("submarket", "")),
            "submarket": cs.get("submarket"),
            "borough": "Manhattan",
            "zip": cs.get("zip", ""),
            "bbl": None,
            "keys": cs["keys"],
            "stories": cs.get("stories"),
            "yearBuilt": cs.get("yearBuilt"),
            "yearRenovated": cs.get("yearRenovated"),
            "meetingRooms": cs.get("meetingRooms"),
            "totalMeetingSpace": cs.get("totalMeetingSpace"),
            "locationType": cs.get("locationType"),
            "owner": None,
            "lat": None,
            "lng": None,
            "marketValue": None,
            "taxableAV": None,
            "currentTaxRate": None,
            "annualTax": None,
            "annualTaxBeforeAbatements": None,
            "grossSqft": None,
            "historicalTaxPerKey": [],
            "notes": ""
        }

        if best and best_score >= 10:
            record["bbl"] = best["bbl"]
            record["owner"] = best["owner"]
            record["buildingClass"] = best["bldg_class"]
            record["buildingClassDesc"] = CLASS_DESC.get(best["bldg_class"], best["bldg_class"])
            record["marketValue"] = best["market_value"]
            record["taxableAV"] = best["taxable_av"]
            record["annualTax"] = best["tax_before"] or best.get("tax_after")
            record["annualTaxBeforeAbatements"] = best["tax_before"]
            record["grossSqft"] = best["gross_sqft"]
            if record["taxableAV"] and record["taxableAV"] > 0 and record["annualTax"]:
                record["currentTaxRate"] = round(record["annualTax"] / record["taxableAV"], 5)
            matched += 1

        results.append(record)

    return results, matched


def main():
    print("=" * 60)
    print("CoStar + DOF Hotel Tax Data Merger")
    print("=" * 60)

    # Load CoStar
    with open("costar_hotels.json") as f:
        costar = json.load(f)
    print(f"\nCoStar hotels (100+ keys): {len(costar)}")
    print(f"Total keys: {sum(h['keys'] for h in costar):,}")

    # Fetch DOF
    print("\nFetching DOF data from NYC Open Data...")
    dof = fetch_dof_hotels()
    print(f"Total DOF hotel properties: {len(dof)}")
    manhattan_dof = [d for d in dof if d["boro"] == "1"]
    print(f"Manhattan DOF hotels: {len(manhattan_dof)}")

    # Match
    print("\nMatching CoStar -> DOF...")
    hotels, matched = match_hotels(costar, dof)
    print(f"Matched: {matched}/{len(costar)} ({matched/len(costar)*100:.1f}%)")

    # Stats
    with_tax = [h for h in hotels if h["annualTax"] and h["annualTax"] > 0]
    print(f"\nHotels with tax data: {len(with_tax)}")
    if with_tax:
        total_tax = sum(h["annualTax"] for h in with_tax)
        print(f"Total annual hotel tax: ${total_tax:,.0f}")

        tpk = sorted([h["annualTax"] / h["keys"] for h in with_tax if h["keys"]])
        if tpk:
            print(f"\nTax per Key:")
            print(f"  Min:    ${min(tpk):,.0f}")
            print(f"  25th:   ${tpk[len(tpk)//4]:,.0f}")
            print(f"  Median: ${tpk[len(tpk)//2]:,.0f}")
            print(f"  75th:   ${tpk[3*len(tpk)//4]:,.0f}")
            print(f"  Max:    ${max(tpk):,.0f}")

    # Output
    output = {
        "lastUpdated": datetime.now().strftime("%Y-%m-%d"),
        "metadata": {
            "description": "NYC Hotels 100+ Keys — Property Tax per Key Benchmarking",
            "source": "CoStar Export + NYC DOF via Open Data API",
            "criteria": {
                "geography": "Manhattan, New York City",
                "minKeys": 100,
                "totalHotels": len(hotels),
                "matchedWithTaxData": matched,
                "dataPoints": ["Key count", "Market value", "Taxable AV", "Annual tax",
                               "Tax per key", "MV per key", "Brand", "Hotel class"]
            }
        },
        "hotels": hotels
    }

    with open("hotel-comps.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved hotel-comps.json ({len(hotels)} hotels)")

    # Top 10 by tax per key
    if with_tax:
        print("\n--- Top 10 by Tax/Key (highest) ---")
        ranked = sorted(with_tax, key=lambda h: h["annualTax"]/h["keys"], reverse=True)
        for h in ranked[:10]:
            tpk = h["annualTax"] / h["keys"]
            print(f"  ${tpk:>8,.0f}/key  {h['hotelName'][:40]:<40s}  {h['keys']} keys  ${h['annualTax']:>12,.0f} tax")

        print("\n--- Top 10 by Tax/Key (lowest) ---")
        for h in ranked[-10:]:
            tpk = h["annualTax"] / h["keys"]
            print(f"  ${tpk:>8,.0f}/key  {h['hotelName'][:40]:<40s}  {h['keys']} keys  ${h['annualTax']:>12,.0f} tax")


if __name__ == "__main__":
    main()
