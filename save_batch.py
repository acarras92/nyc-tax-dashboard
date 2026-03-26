import json, sys
progress = json.load(open('scrape_progress.json'))
batch = json.load(open('batch_temp.json'))
progress.update(batch)
with open('scrape_progress.json', 'w') as f:
    json.dump(progress, f, indent=2)
scraped = sum(1 for v in progress.values() if v.get('scraped'))
errors = sum(1 for v in progress.values() if 'error' in v)
print(f'Total: {len(progress)} | Scraped: {scraped} | Errors: {errors}')
