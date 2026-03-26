// Usage: node _save_batch.js
// Reads _batch_tmp.json and merges into fix_progress.json
const fs = require('fs');
const result = JSON.parse(fs.readFileSync('_batch_tmp.json','utf8'));
const progress = JSON.parse(fs.readFileSync('fix_progress.json','utf8'));
Object.assign(progress, result);
fs.writeFileSync('fix_progress.json', JSON.stringify(progress, null, 2));
console.log('Saved. Total:', Object.keys(progress).length, '/ 155');
console.log('Successes:', Object.values(progress).filter(v=>v.scraped).length);
console.log('Errors:', Object.values(progress).filter(v=>v.error).length);
