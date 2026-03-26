// Template: Batch scraper for NYC DOF - called via browser_run_code
// Replace BATCH_ADDRESSES with the actual array of addresses
async function scrapeBatch(page, hotels) {
  function parseAddress(raw) {
    let addr = raw.trim();
    addr = addr.replace(/^(\d+)([A-Za-z])(\s)/, '$1$2 ');
    const m = addr.match(/^([\d]+-?[\d]*[A-Za-z]?)\s+(.+)$/);
    if (!m) return { number: '', street: addr };
    let number = m[1], street = m[2];
    street = street.replace(/\bW\b(?!\s+Broadway)/gi, 'West').replace(/\bE\b/gi, 'East')
                   .replace(/\bN\b/gi, 'North').replace(/\bS\b(?!\s|$)/gi, 'South');
    street = street.replace(/(\d+)(st|nd|rd|th)\b/gi, '$1');
    street = street.replace(/\b(Street|St|Boulevard|Blvd|Road|Rd|Drive|Dr|Place|Pl|Lane|Ln|Way|Court|Ct|Terrace|Ter|Plaza|Plz|Square|Sq)\b\.?$/gi, '').trim();
    street = street.replace(/\bAve\b\.?$/gi, '').trim();
    street = street.replace(/\s+/g, ' ').trim();
    return { number, street };
  }

  async function scrapeAssessment() {
    return await page.evaluate(() => {
      const r = {};
      for (const row of document.querySelectorAll('table tr')) {
        const c = Array.from(row.querySelectorAll('td'));
        if (c.length >= 4) { const d=c[1]?.textContent?.trim(),t=c[3]?.textContent?.trim(); if(d==='ESTIMATED MARKET VALUE')r.marketValue=t; if(d==='MARKET AV')r.marketAV=t; if(d==='TRANS AV')r.transAV=t; }
        if (c.length === 2) { const l=c[0]?.textContent?.trim(),v=c[1]?.textContent?.trim(); if(l==='Owner Name')r.owner=v; if(l==='Building Class')r.buildingClass=v; if(l==='Tax Class')r.taxClass=v; if(l==='Stories')r.stories=v; if(l?.includes('Taxes Will Be Based On'))r.taxableAV=v; }
      }
      const h1=document.querySelector('h1'); if(h1){const t=h1.textContent;const bm=t.match(/Block:\s*(\d+)/),lm=t.match(/Lot:\s*(\d+)/),am=t.match(/^(.+?)(?:\s+Borough:)/);if(bm)r.block=bm[1];if(lm)r.lot=lm[1];if(am)r.dofAddress=am[1].trim();}
      const mv=[]; for(const table of document.querySelectorAll('table')){if(table.textContent.includes('Market Value History')){for(const tr of table.querySelectorAll('tr')){const td=tr.querySelectorAll('td');if(td.length>=2){const y=td[0]?.textContent?.trim(),v=td[1]?.textContent?.trim();if(/^\d{4}\s*-\s*\d{4}$/.test(y))mv.push({year:y,value:v});}}break;}} r.mvHistory=mv;
      return r;
    });
  }

  const results = {};
  for (const addr of hotels) {
    const p = parseAddress(addr);
    try {
      await page.goto('https://a836-pts-access.nyc.gov/care/search/commonsearch.aspx?mode=address', { waitUntil: 'networkidle', timeout: 15000 });
      await page.selectOption('#Select1', { label: '(1) Manhattan' });
      await page.fill('#inpNumber', p.number);
      await page.fill('#inpStreet', p.street);
      await page.click('#btSearch');
      await page.waitForLoadState('networkidle', { timeout: 15000 });
      const row = await page.$('tr[class*="SearchResult"]');
      const isDet = page.url().includes('Datalet');
      if (!row && !isDet) { results[addr] = { error: 'No results' }; continue; }
      if (row) { await row.click(); await page.waitForLoadState('networkidle', { timeout: 15000 }); }
      const aLink = await page.$('a:has-text("2025-2026 Final")');
      if (aLink) { await aLink.click(); await page.waitForLoadState('networkidle', { timeout: 15000 }); }
      const data = await scrapeAssessment();
      data.scraped = true;
      results[addr] = data;
    } catch (e) { results[addr] = { error: e.message?.substring(0, 150) }; }
  }
  return JSON.stringify(results);
}
