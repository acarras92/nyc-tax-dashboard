"""
Batch orchestrator: reads costar_hotels.json, generates JS batch files for Playwright MCP,
and merges scraped results into hotel-comps.json.

Usage:
  python batch_scrape.py generate   # Generate JS batch files
  python batch_scrape.py merge      # Merge all batch results into hotel-comps.json
  python batch_scrape.py status     # Show scrape progress
"""

import json
import os
import re
import sys
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(SCRIPT_DIR, "costar_hotels.json")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "hotel-comps.json")
PROGRESS_FILE = os.path.join(SCRIPT_DIR, "scrape_progress.json")
BATCH_DIR = os.path.join(SCRIPT_DIR, "batches")

BATCH_SIZE = 20  # Hotels per batch
TC4_TAX_RATE = 10.646 / 100  # NYC TC4 rate 2025/26


def safe_int(text):
    if not text:
        return None
    cleaned = re.sub(r'[,$\s]', '', str(text))
    try:
        return int(cleaned)
    except ValueError:
        return None


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def generate_batches():
    """Generate JS batch files and a master batch list."""
    with open(INPUT_FILE, "r") as f:
        hotels = json.load(f)

    progress = load_progress()
    pending = [h for h in hotels if h["address"] not in progress or not progress[h["address"]].get("scraped")]

    print(f"Total hotels: {len(hotels)}")
    print(f"Already scraped: {len(hotels) - len(pending)}")
    print(f"Pending: {len(pending)}")

    os.makedirs(BATCH_DIR, exist_ok=True)

    batches = []
    for i in range(0, len(pending), BATCH_SIZE):
        batch = pending[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        addresses = [h["address"] for h in batch]
        batch_file = os.path.join(BATCH_DIR, f"batch_{batch_num:03d}.json")
        with open(batch_file, "w") as f:
            json.dump(addresses, f)
        batches.append({"batch": batch_num, "count": len(addresses), "file": batch_file})
        print(f"  Batch {batch_num}: {len(addresses)} hotels")

    print(f"\nGenerated {len(batches)} batches in {BATCH_DIR}/")
    print(f"Total addresses to scrape: {sum(b['count'] for b in batches)}")


def merge_results():
    """Merge all scraped data into hotel-comps.json."""
    with open(INPUT_FILE, "r") as f:
        hotels = json.load(f)

    progress = load_progress()
    merged = []

    for hotel in hotels:
        addr = hotel["address"]
        dof = progress.get(addr, {})

        taxable_av = safe_int(dof.get("taxableAV")) or safe_int(dof.get("transAV"))
        market_value = safe_int(dof.get("marketValue"))
        annual_tax = round(taxable_av * TC4_TAX_RATE) if taxable_av else None

        bldg_class_raw = dof.get("buildingClass", "")
        bldg_class_code = bldg_class_raw.split(" - ")[0].strip() if bldg_class_raw else None
        bldg_class_desc = bldg_class_raw.split(" - ", 1)[1].strip() if " - " in bldg_class_raw else bldg_class_raw

        hc = (hotel.get("hotelClass") or "").lower().replace(" ", "-")
        keys = hotel.get("keys")
        tax_per_key = round(annual_tax / keys) if annual_tax and keys else None
        mv_per_key = round(market_value / keys) if market_value and keys else None

        block = dof.get("block")
        lot = dof.get("lot")
        bbl = f"1-{block.zfill(5)}-{lot.zfill(4)}" if block and lot else None

        mv_history = []
        for h in dof.get("mvHistory", []):
            mv = safe_int(h.get("value"))
            if mv:
                mv_history.append({"year": h["year"], "marketValue": mv})

        entry = {
            "id": None,
            "hotelName": hotel.get("hotelName"),
            "brand": hotel.get("brand"),
            "parentCompany": hotel.get("parentCompany"),
            "hotelClass": hc,
            "hotelClassDisplay": hotel.get("hotelClass"),
            "scale": hotel.get("scale"),
            "operationType": hotel.get("operationType"),
            "buildingClass": bldg_class_code,
            "buildingClassDesc": bldg_class_desc,
            "address": addr,
            "dofAddress": dof.get("dofAddress"),
            "neighborhood": None,
            "submarket": hotel.get("submarket"),
            "borough": "Manhattan",
            "zip": hotel.get("zip"),
            "bbl": bbl,
            "keys": keys,
            "stories": safe_int(dof.get("stories")) or hotel.get("stories"),
            "yearBuilt": hotel.get("yearBuilt"),
            "yearRenovated": hotel.get("yearRenovated"),
            "meetingRooms": hotel.get("meetingRooms"),
            "totalMeetingSpace": hotel.get("totalMeetingSpace"),
            "locationType": hotel.get("locationType"),
            "owner": dof.get("owner"),
            "lat": None,
            "lng": None,
            "marketValue": market_value,
            "marketAV": safe_int(dof.get("marketAV")),
            "taxableAV": taxable_av,
            "taxClass": dof.get("taxClass"),
            "annualTax": annual_tax,
            "taxPerKey": tax_per_key,
            "mvPerKey": mv_per_key,
            "grossSqft": None,
            "marketValueHistory": mv_history,
            "scrapeStatus": "success" if dof.get("scraped") else ("error" if dof.get("error") else "pending"),
            "scrapeError": dof.get("error"),
            "notes": ""
        }
        merged.append(entry)

    # Sort by annual tax descending
    merged.sort(key=lambda h: h.get("annualTax") or 0, reverse=True)
    for i, h in enumerate(merged, 1):
        h["id"] = f"COMP-{i:04d}"

    matched = sum(1 for h in merged if h.get("marketValue"))
    errors = sum(1 for h in merged if h.get("scrapeStatus") == "error")
    pending_count = sum(1 for h in merged if h.get("scrapeStatus") == "pending")

    output = {
        "lastUpdated": datetime.now().strftime("%Y-%m-%d"),
        "metadata": {
            "description": "NYC Hotels 100+ Keys - Property Tax per Key Benchmarking",
            "source": "CoStar Export + NYC DOF Website (Playwright scrape)",
            "scrapedAt": datetime.now().isoformat(),
            "criteria": {
                "geography": "Manhattan, New York City",
                "minKeys": 100,
                "totalHotels": len(hotels),
                "matchedWithTaxData": matched,
                "errors": errors,
                "pending": pending_count,
                "assessmentYear": "2025-2026 Final",
                "tc4TaxRate": TC4_TAX_RATE,
                "dataPoints": [
                    "Key count", "Market value", "Market AV", "Taxable AV",
                    "Annual tax", "Tax per key", "MV per key",
                    "Brand", "Hotel class", "Building class", "Owner"
                ]
            }
        },
        "hotels": merged
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Merged {len(hotels)} hotels into {OUTPUT_FILE}")
    print(f"  Matched: {matched}")
    print(f"  Errors: {errors}")
    print(f"  Pending: {pending_count}")


def show_status():
    """Show scrape progress."""
    with open(INPUT_FILE, "r") as f:
        hotels = json.load(f)

    progress = load_progress()

    scraped = sum(1 for h in hotels if h["address"] in progress and progress[h["address"]].get("scraped"))
    errored = sum(1 for h in hotels if h["address"] in progress and progress[h["address"]].get("error"))
    pending = len(hotels) - scraped - errored

    print(f"Total: {len(hotels)}")
    print(f"Scraped: {scraped}")
    print(f"Errors: {errored}")
    print(f"Pending: {pending}")

    if errored > 0:
        print(f"\nFailed addresses:")
        for h in hotels:
            addr = h["address"]
            if addr in progress and progress[addr].get("error"):
                print(f"  {h['hotelName']} - {addr}: {progress[addr]['error'][:60]}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "generate":
        generate_batches()
    elif cmd == "merge":
        merge_results()
    elif cmd == "status":
        show_status()
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python batch_scrape.py [generate|merge|status]")
