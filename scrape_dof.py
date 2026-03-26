"""
NYC DOF Property Tax Scraper via Playwright
Scrapes property assessment data from the NYC DOF website for each hotel in costar_hotels.json.
Navigates the ASP.NET web forms app: address search → results → property detail → assessment page.
"""

import json
import re
import sys
import time
import os
import io
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(SCRIPT_DIR, "costar_hotels.json")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "hotel-comps.json")
PROGRESS_FILE = os.path.join(SCRIPT_DIR, "scrape_progress.json")

SEARCH_URL = "https://a836-pts-access.nyc.gov/care/search/commonsearch.aspx?mode=address"
DISCLAIMER_URL = "https://a836-pts-access.nyc.gov/care/Search/Disclaimer.aspx"

# NYC TC4 tax rate for 2025/26 (per $100 of AV)
TC4_TAX_RATE = 10.646 / 100  # 10.646%


def parse_address(raw_addr):
    """
    Parse a CoStar address like '400 W 42nd St' into (number, street) for DOF search.
    Rules:
      - Spell out directional: W→West, E→East, N→North, S→South
      - Remove ordinals: 42nd→42, 56th→56, 33rd→33, 21st→21
      - Remove street suffix: St, Ave, Blvd, etc.
    """
    addr = raw_addr.strip()

    # Extract house number (could be hyphenated like 22-35)
    m = re.match(r'^([\d-]+)\s+(.+)$', addr)
    if not m:
        return None, addr

    number = m.group(1)
    street = m.group(2).strip()

    # Spell out directional abbreviations (standalone words)
    dir_map = {
        r'\bW\b': 'West', r'\bE\b': 'East',
        r'\bN\b': 'North', r'\bS\b': 'South',
    }
    for pat, repl in dir_map.items():
        street = re.sub(pat, repl, street, flags=re.IGNORECASE)

    # Remove ordinal suffixes from numbers: 42nd→42, 56th→56, etc.
    street = re.sub(r'(\d+)(st|nd|rd|th)\b', r'\1', street, flags=re.IGNORECASE)

    # Remove street type suffixes
    suffixes = r'\b(Street|St|Avenue|Ave|Boulevard|Blvd|Road|Rd|Drive|Dr|Place|Pl|Lane|Ln|Way|Court|Ct|Terrace|Ter|Plaza|Plz|Square|Sq|Circle|Cir)\b\.?'
    street = re.sub(suffixes, '', street, flags=re.IGNORECASE).strip()

    # Clean up extra spaces
    street = re.sub(r'\s+', ' ', street).strip()

    # Handle "Avenue of the Americas" / "Avenue of Americas" → "6 Avenue" or keep as-is
    # Handle "Broadway" — no number prefix needed, just return as-is
    # Handle "Park Avenue South" → "Park South" after removing "Avenue"
    # Actually, let's be smarter: if street is something like "Park" after removing "Ave",
    # the DOF search should still find it.

    return number, street


def safe_int(text):
    """Parse a comma-formatted number string to int."""
    if not text:
        return None
    text = text.strip().replace(",", "").replace("$", "")
    try:
        return int(text)
    except ValueError:
        return None


def ensure_search_page(page):
    """
    Make sure we're on the address search form.
    Handles disclaimer, navigates if needed.
    """
    max_attempts = 3
    for attempt in range(max_attempts):
        url = page.url.lower()
        print(f"    [debug] URL: {page.url}", flush=True) if attempt > 0 else None

        # If on disclaimer, accept it
        if "disclaimer" in url:
            try:
                page.click("button:has-text('Agree')", timeout=10000)
                page.wait_for_load_state("networkidle", timeout=15000)
                continue  # Re-check URL
            except PWTimeout:
                pass

        # Check if search form is present
        select = page.query_selector("#Select1")
        if select:
            return  # We're on the search page

        # Try clicking the address search link
        link = page.query_selector("a[href*='commonsearch.aspx?mode=address']")
        if link:
            try:
                link.click()
                page.wait_for_load_state("networkidle", timeout=10000)
                continue
            except PWTimeout:
                pass

        # Fallback: navigate directly
        page.goto(SEARCH_URL, wait_until="networkidle", timeout=30000)

    # Final check
    if not page.query_selector("#Select1"):
        # Debug: print page content
        print(f"    [debug] Final URL: {page.url}", flush=True)
        print(f"    [debug] Title: {page.title()}", flush=True)
        body = page.query_selector("body")
        if body:
            text = body.inner_text()[:500]
            print(f"    [debug] Body: {text[:200]}", flush=True)


def search_address(page, number, street):
    """
    Fill in the address search form and submit.
    Returns True if search was submitted successfully.
    """
    ensure_search_page(page)

    # Wait for the form to be ready
    page.wait_for_selector("#Select1", timeout=10000)

    # Select borough = Manhattan (select id=Select1, name=inpUnit)
    page.select_option("#Select1", label="(1) Manhattan")

    # Fill number and street (ids: inpNumber, inpStreet)
    page.fill("#inpNumber", number or "")
    page.fill("#inpStreet", street)

    # Click search
    page.click("#btSearch")
    page.wait_for_load_state("networkidle", timeout=15000)

    return True


def get_search_results(page):
    """
    Parse the search results table.
    Returns list of dicts: [{bbl, owner, address}, ...]
    """
    results = []
    rows = page.query_selector_all("table[id*='GridView'] tr.SearchResults, table[id*='GridView'] tr.SearchResultsAlt, table[id*='gridview'] tr.SearchResults, table[id*='gridview'] tr.SearchResultsAlt")

    if not rows:
        # Try alternative: any clickable rows in the results table
        rows = page.query_selector_all("tr[class*='SearchResult']")

    if not rows:
        # Check if we landed directly on a property detail page (single result)
        if "Datalet" in page.url or page.query_selector("text=Property Owner"):
            return [{"direct": True}]
        return []

    for row in rows:
        cells = row.query_selector_all("td")
        if len(cells) >= 3:
            results.append({
                "bbl": (cells[0].inner_text() or "").strip(),
                "owner": (cells[1].inner_text() or "").strip(),
                "address": (cells[2].inner_text() or "").strip(),
                "element": row,
            })

    return results


def pick_best_result(results, target_number):
    """
    From search results, pick the best match.
    Prefer results whose address starts with the target house number.
    For condos, prefer the first unit or the one without apartment suffixes.
    """
    if not results:
        return None
    if len(results) == 1:
        return results[0]

    # Prefer exact number match without apartment suffix
    for r in results:
        addr = r.get("address", "")
        if addr.startswith(target_number + " ") and "#" not in addr:
            return r

    # Prefer exact number match (first one)
    for r in results:
        addr = r.get("address", "")
        if addr.startswith(target_number + " "):
            return r

    # Fall back to first result
    return results[0]


def scrape_property_info(page):
    """
    Scrape the Property Info page for basic data.
    Returns dict with owner, building_class, tax_class, block, lot, bbl, stories.
    """
    info = {}

    # Get header info (address, borough, block, lot)
    header = page.query_selector("h1")
    if header:
        header_text = header.inner_text()
        # Extract block and lot from header
        block_m = re.search(r'Block:\s*(\d+)', header_text)
        lot_m = re.search(r'Lot:\s*(\d+)', header_text)
        if block_m:
            info["block"] = block_m.group(1)
        if lot_m:
            info["lot"] = lot_m.group(1)
        # Extract address
        addr_m = re.match(r'(.+?)(?:\s+Borough:)', header_text)
        if addr_m:
            info["dof_address"] = addr_m.group(1).strip()

    # Scrape the key-value pairs from tables
    rows = page.query_selector_all("table tr")
    for row in rows:
        cells = row.query_selector_all("td")
        if len(cells) == 2:
            label = (cells[0].inner_text() or "").strip()
            value = (cells[1].inner_text() or "").strip()

            if label == "Owner Name":
                info["owner"] = value
            elif label == "Building Class":
                info["building_class"] = value
            elif label == "Tax Class":
                info["tax_class"] = value
            elif label == "Tax Year":
                info["tax_year"] = value

    # Build BBL
    if "block" in info and "lot" in info:
        info["bbl"] = f"1-{info['block'].zfill(5)}-{info['lot'].zfill(4)}"

    return info


def scrape_assessment(page):
    """
    Scrape the 2025-2026 Final Assessment page.
    Returns dict with market_value, market_av, trans_av, taxable_av, stories, owner, building_class.
    """
    data = {}

    # Scrape all table rows for key-value pairs
    rows = page.query_selector_all("table tr")
    for row in rows:
        cells = row.query_selector_all("td")
        if len(cells) >= 2:
            label = (cells[0].inner_text() or "").strip()

            # Assessment Information section (has 4 columns: blank, desc, land, total)
            if len(cells) >= 4:
                desc = (cells[1].inner_text() or "").strip()
                total = (cells[3].inner_text() or "").strip()

                if desc == "ESTIMATED MARKET VALUE":
                    data["market_value"] = safe_int(total)
                elif desc == "MARKET AV":
                    data["market_av"] = safe_int(total)
                elif desc == "TRANS AV":
                    data["trans_av"] = safe_int(total)

            # Two-column key-value pairs
            if len(cells) == 2:
                value = (cells[1].inner_text() or "").strip()

                if label == "Owner Name":
                    data["owner"] = value
                elif label == "Building Class":
                    data["building_class"] = value
                elif label == "Tax Class":
                    data["tax_class"] = value
                elif label == "Stories":
                    data["stories"] = safe_int(value)
                elif label == "Number of Buildings":
                    data["num_buildings"] = safe_int(value)
                elif "Your 2025/26 Taxes Will Be Based On" in label or "Taxes Will Be Based On" in label:
                    data["taxable_av"] = safe_int(value)

    # Market value history
    mv_history = []
    table_texts = page.query_selector_all("table")
    for table in table_texts:
        text = table.inner_text()
        if "Market Value History" in text:
            hist_rows = table.query_selector_all("tr")
            for hr in hist_rows:
                hcells = hr.query_selector_all("td")
                if len(hcells) >= 2:
                    year_text = (hcells[0].inner_text() or "").strip()
                    val_text = (hcells[1].inner_text() or "").strip()
                    if re.match(r'\d{4}\s*-\s*\d{4}', year_text):
                        mv = safe_int(val_text)
                        if mv:
                            mv_history.append({"year": year_text, "marketValue": mv})
            break

    if mv_history:
        data["market_value_history"] = mv_history

    return data


def navigate_to_assessment(page):
    """Navigate from property detail to the 2025-2026 Final Assessment page."""
    try:
        link = page.query_selector("a:has-text('2025-2026 Final')")
        if link:
            link.click()
            page.wait_for_load_state("networkidle", timeout=15000)
            return True
    except PWTimeout:
        pass
    return False


def scrape_one_hotel(page, hotel, index, total):
    """
    Scrape DOF data for a single hotel. Returns a result dict or None.
    """
    addr = hotel.get("address", "")
    name = hotel.get("hotelName", addr)
    print(f"  [{index+1}/{total}] {name} — {addr}")

    number, street = parse_address(addr)
    if not number:
        print(f"    ⚠ Could not parse address: {addr}")
        return {"error": f"Could not parse address: {addr}", "address": addr}

    try:
        # Search
        search_address(page, number, street)

        # Check for results
        results = get_search_results(page)

        if not results:
            # Try without the number — sometimes helps for avenue addresses
            print(f"    No results for '{number} {street}', trying broader search...")
            # Try with just street name
            search_address(page, "", street)
            results = get_search_results(page)

        if not results:
            print(f"    ✗ No results found")
            return {"error": "No results found", "address": addr}

        # If we landed directly on a detail page (single result)
        if results[0].get("direct"):
            pass  # Already on detail page
        else:
            # Pick best result and click it
            best = pick_best_result(results, number)
            if best and best.get("element"):
                best["element"].click()
                page.wait_for_load_state("networkidle", timeout=15000)

        # Now on property detail page — navigate to assessment
        if navigate_to_assessment(page):
            data = scrape_assessment(page)
        else:
            # Try scraping whatever is on the current page
            data = scrape_property_info(page)

        if not data.get("market_value") and not data.get("owner"):
            print(f"    ⚠ Scraped but got minimal data")
        else:
            mv = data.get("market_value")
            tav = data.get("taxable_av") or data.get("trans_av")
            owner = data.get("owner", "N/A")
            print(f"    ✓ MV: ${mv:,}" if mv else "    ✓ MV: N/A", end="")
            print(f"  TAV: ${tav:,}" if tav else "  TAV: N/A", end="")
            print(f"  Owner: {owner[:40]}")

        data["address"] = addr
        data["scraped"] = True
        return data

    except Exception as e:
        print(f"    ✗ Error: {e}")
        return {"error": str(e), "address": addr}


def load_progress():
    """Load previously scraped results to allow resumption."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_progress(progress):
    """Save progress for resumption."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def build_output(hotels, scraped_data):
    """Merge CoStar hotel data with scraped DOF data into hotel-comps.json format."""
    merged = []

    for hotel in hotels:
        addr = hotel["address"]
        dof = scraped_data.get(addr, {})

        # Calculate annual tax from taxable AV
        taxable_av = dof.get("taxable_av") or dof.get("trans_av")
        annual_tax = round(taxable_av * TC4_TAX_RATE) if taxable_av else None
        market_value = dof.get("market_value")

        # Normalize building class
        bldg_class_raw = dof.get("building_class", "")
        bldg_class_code = bldg_class_raw.split(" - ")[0].strip() if bldg_class_raw else None
        bldg_class_desc = bldg_class_raw.split(" - ")[1].strip() if " - " in bldg_class_raw else bldg_class_raw

        # Normalize hotel class for filtering
        hc = (hotel.get("hotelClass") or "").lower().replace(" ", "-")

        keys = hotel.get("keys")
        tax_per_key = round(annual_tax / keys) if annual_tax and keys else None
        mv_per_key = round(market_value / keys) if market_value and keys else None

        entry = {
            "id": None,  # assigned later
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
            "dofAddress": dof.get("dof_address"),
            "neighborhood": None,
            "submarket": hotel.get("submarket"),
            "borough": "Manhattan",
            "zip": hotel.get("zip"),
            "bbl": dof.get("bbl"),
            "keys": keys,
            "stories": dof.get("stories") or hotel.get("stories"),
            "yearBuilt": hotel.get("yearBuilt"),
            "yearRenovated": hotel.get("yearRenovated"),
            "meetingRooms": hotel.get("meetingRooms"),
            "totalMeetingSpace": hotel.get("totalMeetingSpace"),
            "locationType": hotel.get("locationType"),
            "owner": dof.get("owner"),
            "lat": None,
            "lng": None,
            "marketValue": market_value,
            "marketAV": dof.get("market_av"),
            "taxableAV": taxable_av,
            "taxClass": dof.get("tax_class"),
            "annualTax": annual_tax,
            "taxPerKey": tax_per_key,
            "mvPerKey": mv_per_key,
            "grossSqft": None,
            "marketValueHistory": dof.get("market_value_history", []),
            "scrapeStatus": "success" if dof.get("scraped") else ("error" if dof.get("error") else "pending"),
            "scrapeError": dof.get("error"),
            "notes": ""
        }

        merged.append(entry)

    # Sort by annual tax descending
    merged.sort(key=lambda h: h.get("annualTax") or 0, reverse=True)

    # Assign sequential IDs
    for i, h in enumerate(merged, 1):
        h["id"] = f"COMP-{i:04d}"

    matched = sum(1 for h in merged if h.get("marketValue"))
    output = {
        "lastUpdated": datetime.now().strftime("%Y-%m-%d"),
        "metadata": {
            "description": "NYC Hotels 100+ Keys — Property Tax per Key Benchmarking",
            "source": "CoStar Export + NYC DOF Website (Playwright scrape)",
            "scrapedAt": datetime.now().isoformat(),
            "criteria": {
                "geography": "Manhattan, New York City",
                "minKeys": 100,
                "totalHotels": len(hotels),
                "matchedWithTaxData": matched,
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
    return output


def main():
    # Parse args
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            pass

    # Load hotels
    with open(INPUT_FILE, "r") as f:
        hotels = json.load(f)

    if limit:
        hotels_to_scrape = hotels[:limit]
        print(f"Scraping first {limit} of {len(hotels)} hotels")
    else:
        hotels_to_scrape = hotels
        print(f"Scraping all {len(hotels)} hotels")

    # Load any previous progress
    progress = load_progress()
    already_done = sum(1 for h in hotels_to_scrape if h["address"] in progress and progress[h["address"]].get("scraped"))
    if already_done > 0:
        print(f"  ({already_done} already scraped, will skip)")

    print("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        # Mask webdriver detection
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = context.new_page()

        # Accept disclaimer once
        page.goto(SEARCH_URL, wait_until="networkidle", timeout=30000)
        ensure_search_page(page)

        total = len(hotels_to_scrape)
        for i, hotel in enumerate(hotels_to_scrape):
            addr = hotel["address"]

            # Skip if already scraped
            if addr in progress and progress[addr].get("scraped"):
                continue

            result = scrape_one_hotel(page, hotel, i, total)
            if result:
                progress[addr] = result

            # Save progress every 5 hotels
            if (i + 1) % 5 == 0:
                save_progress(progress)

            # Small delay to be polite
            time.sleep(0.5)

        browser.close()

    # Save final progress
    save_progress(progress)

    # Build and save output (use ALL hotels, not just the batch)
    with open(INPUT_FILE, "r") as f:
        all_hotels = json.load(f)

    output = build_output(all_hotels, progress)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    matched = output["metadata"]["criteria"]["matchedWithTaxData"]
    print()
    print("=" * 60)
    print(f"Done! {matched}/{len(all_hotels)} hotels matched with tax data")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
