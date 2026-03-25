# NYC Property Tax Dashboard — Highgate Capital

## Project Overview
Live GitHub Pages dashboard tracking NYC hotel property tax assessments for deal diligence and portfolio monitoring.

- **Repo:** acarras92/nyc-tax-dashboard
- **Live URL:** https://acarras92.github.io/nyc-tax-dashboard/
- **Owner:** Andrew Carras (acarras92@gmail.com) — Real estate investor at Highgate Capital

## Architecture
- `index.html` — Main property tax tracker dashboard (dark theme, single-file HTML+CSS+JS)
- `hotel-comps.html` — Phase 2: Hotel tax-per-key comp page (328 hotels, 100+ keys)
- `properties.json` — Manually tracked properties (portfolio/deal targets)
- `hotel-comps.json` — Hotel comp data (CoStar export + DOF tax data)
- `costar_hotels.json` — Clean extract of CoStar hotel data (328 hotels with key counts, brands, classes)
- `scrape_hotels.py` — Bulk scraper using NYC Open Data API
- `merge_costar_dof.py` — Merges CoStar data with DOF tax records

## Data Sources
- **NYC DOF Property Tax Portal:** https://a836-pts-access.nyc.gov/care/search/commonsearch.aspx?mode=address
- **NYC Open Data API:** https://data.cityofnewyork.us/resource/8y4t-faws.json (DOF assessment roll)
- **CoStar:** Hotel export with key counts, brands, hotel class, submarkets

## Key Data Fields
Each hotel in hotel-comps.json has: hotelName, brand, parentCompany, hotelClass, keys, address, submarket, borough, bbl, owner, marketValue, taxableAV, currentTaxRate, annualTax, grossSqft, sourceUrl

## Known Issues
- 199/328 hotels matched via API — remaining 129 need Playwright-based DOF scraping
- Some matched records have incorrect tax data (wrong BBL matched or partial records)
- Tax rates showing 100% or values that seem too low are bad matches that need re-scraping
- Park Lane Hotel (36 Central Park S) confirmed wrong — real taxes ~$7MM, API showed ~$1MM

## Conventions
- Always use Playwright MCP to scrape DOF portal for accurate data (not the bulk API)
- Every property must have a `sourceUrl` linking to its DOF assessment page for verification
- Dashboard uses dark theme (#0f1419 background)
- Deploy by pushing to main branch (GitHub Pages auto-deploys)

## GitHub Pages Deploy
```
git add -A && git commit -m "description" && git push
```
Pages auto-deploys from main branch. Live at https://acarras92.github.io/nyc-tax-dashboard/
