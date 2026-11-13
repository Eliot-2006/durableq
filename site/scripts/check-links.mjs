// Checks every internal link, asset and #fragment in the built site. No dependencies.
// Usage: node scripts/check-links.mjs [distDir] [basePath]
import { readdirSync, readFileSync, existsSync, statSync } from 'node:fs';
import { join, relative, sep } from 'node:path';
import { posix } from 'node:path';

const dist = process.argv[2] ?? 'dist';
const base = process.argv[3] ?? '/durableq/';
if (!existsSync(dist)) { console.error(`no such directory: ${dist} (run the build first)`); process.exit(2); }

const walk = (d) => readdirSync(d).flatMap((f) => (statSync(join(d, f)).isDirectory() ? walk(join(d, f)) : [join(d, f)]));
const files = walk(dist);
const pages = files.filter((f) => f.endsWith('.html'));
const idsCache = new Map();
const ids = (file) => {
  if (!idsCache.has(file)) {
    const html = readFileSync(file, 'utf8');
    idsCache.set(file, new Set([...html.matchAll(/\s(?:id|name)="([^"]*)"/g)].map((m) => m[1])));
  }
  return idsCache.get(file);
};

const errors = [];
let checked = 0;
for (const page of pages) {
  const rel = relative(dist, page).split(sep).join('/');
  const pageUrl = base + rel.replace(/index\.html$/, '');
  const html = readFileSync(page, 'utf8');
  const refs = [];
  for (const m of html.matchAll(/\s(?:href|src|poster)="([^"]*)"/g)) refs.push(m[1]);
  for (const m of html.matchAll(/\ssrcset="([^"]*)"/g)) for (const part of m[1].split(',')) refs.push(part.trim().split(/\s+/)[0]);

  for (const ref of refs) {
    if (!ref || /^[a-z][a-z0-9+.-]*:/i.test(ref) || ref.startsWith('//')) continue; // external, mailto:, data:
    checked++;
    const [pathAndQuery, hash = ''] = ref.split('#');
    const path = pathAndQuery.split('?')[0];
    let url;
    if (path === '') url = pageUrl;
    else if (path.startsWith('/')) {
      if (!path.startsWith(base)) { errors.push(`${rel}: "${ref}" is root-relative but outside the base path ${base}`); continue; }
      url = path;
    } else url = posix.normalize(posix.join(pageUrl.endsWith('/') ? pageUrl : posix.dirname(pageUrl) + '/', path));

    if (!url.startsWith(base)) { errors.push(`${rel}: "${ref}" resolves outside the base path`); continue; }
    const target = url.slice(base.length);
    const candidates = target === '' || target.endsWith('/') ? [join(dist, target, 'index.html')] : [join(dist, target), join(dist, target, 'index.html')];
    const found = candidates.find((c) => existsSync(c) && statSync(c).isFile());
    if (!found) { errors.push(`${rel}: "${ref}" -> missing ${candidates[0]}`); continue; }
    if (hash && found.endsWith('.html') && !ids(found).has(decodeURIComponent(hash))) errors.push(`${rel}: "${ref}" -> no element with id "${hash}"`);
  }
}

if (errors.length) { console.error(errors.join('\n')); console.error(`\n${errors.length} broken of ${checked} internal references in ${pages.length} pages`); process.exit(1); }
console.log(`ok: ${checked} internal references in ${pages.length} pages`);
