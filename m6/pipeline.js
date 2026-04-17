const http = require('http');
const https = require('https');

function isError(e) {
  if (!e) {
    return false;
  }
  if (e instanceof Error) {
    return true;
  }
  if (typeof e === 'object') {
    return Object.keys(e).length > 0;
  }
  return true;
}

function crawlMap(key, value) {
  const hash = (input) => {
    return globalThis.distribution.util.id.getID(String(input));
  };

  let url = String(key);
  let html = '';

  if (value && typeof value === 'object') {
    if (typeof value.url === 'string' && value.url.length > 0) {
      url = value.url;
    }
    if (typeof value.html === 'string') {
      html = value.html;
    }
  } else if (typeof value === 'string' && /^https?:\/\//i.test(value)) {
    url = value;
  }

  const out = [];
  out.push({[`url:${hash(url)}`]: {url}});

  if (!html) {
    return out;
  }

  const hrefRegex = /href\s*=\s*["']([^"']+)["']/gi;
  let match = hrefRegex.exec(html);
  while (match) {
    const candidate = String(match[1]).trim();
    try {
      const resolved = new URL(candidate, url).toString();
      if (/^https?:\/\//i.test(resolved)) {
        out.push({[`url:${hash(resolved)}`]: {url: resolved}});
      }
    } catch (e) {
      // Ignore malformed links and keep mapper resilient.
    }
    match = hrefRegex.exec(html);
  }

  const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]*>/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

  if (text.length > 0) {
    const docId = hash(url);
    out.push({[`doc:${docId}`]: {docId, url, text}});
  }

  return out;
}

function crawlReduce(key, values) {
  const out = {};

  if (typeof key !== 'string' || !Array.isArray(values) || values.length === 0) {
    return out;
  }

  if (key.startsWith('url:')) {
    const first = values.find((item) => item && typeof item.url === 'string');
    if (!first) {
      return out;
    }

    const url = String(first.url).trim();
    if (!/^https?:\/\//i.test(url)) {
      return out;
    }

    out[key] = {
      kind: 'url',
      url,
      isTxt: /\.txt($|[?#])/i.test(url),
    };
    return out;
  }

  if (key.startsWith('doc:')) {
    let chosen = null;
    values.forEach((item) => {
      if (!item || typeof item.text !== 'string') {
        return;
      }
      if (!chosen || item.text.length > chosen.text.length) {
        chosen = item;
      }
    });

    if (!chosen) {
      return out;
    }

    const text = chosen.text.trim();
    if (!text) {
      return out;
    }

    out[key] = {
      kind: 'doc',
      docId: chosen.docId || key.slice(4),
      url: chosen.url || '',
      text,
    };
    return out;
  }

  return out;
}

function indexMap(key, value) {
  let docId = String(key);
  let text = '';

  if (value && typeof value === 'object') {
    if (value.docId) {
      docId = String(value.docId);
    }
    text = String(value.text || '');
  } else {
    text = String(value || '');
  }

  const tokens = (text.toLowerCase().match(/[a-z0-9]+/g) || []).filter((t) => t.length > 1);
  if (tokens.length === 0) {
    return [];
  }

  const counts = {};
  tokens.forEach((token) => {
    counts[token] = (counts[token] || 0) + 1;
  });

  return Object.entries(counts).map(([term, count]) => ({
    [term]: {
      docId,
      count,
    },
  }));
}

function indexReduce(key, values) {
  const postings = {};

  if (!Array.isArray(values)) {
    return {[key]: postings};
  }

  values.forEach((entry) => {
    if (!entry || !entry.docId) {
      return;
    }
    const docId = String(entry.docId);
    const count = Number(entry.count) || 0;
    postings[docId] = (postings[docId] || 0) + count;
  });

  return {[key]: postings};
}

function putAll(distribution, gid, records, callback) {
  if (!Array.isArray(records) || records.length === 0) {
    callback(null);
    return;
  }

  let pending = records.length;
  let done = false;

  records.forEach((record) => {
    distribution[gid].store.put(record.value, record.key, (e) => {
      if (done) {
        return;
      }
      if (isError(e)) {
        done = true;
        callback(e);
        return;
      }
      pending -= 1;
      if (pending === 0) {
        callback(null);
      }
    });
  });
}

function persistCrawlOutput(distribution, crawlOut, crawlGid, ridxGid, callback) {
  const urlByKey = new Map();
  const docByKey = new Map();
  const urlRecords = [];
  const docRecords = [];

  (crawlOut || []).forEach((entry) => {
    if (!entry || typeof entry !== 'object') {
      return;
    }
    const key = Object.keys(entry)[0];
    const value = entry[key];
    if (!key || !value) {
      return;
    }

    // Accept reducer output shape: {"url:...": {kind: "url", ...}}.
    if (key.startsWith('url:') && value.kind === 'url' && value.url) {
      urlByKey.set(String(value.url), {
        key: String(value.url),
        value: {
          url: String(value.url),
          discovered: true,
          isTxt: value.isTxt === true,
        },
      });
      return;
    }

    // Accept mapper output shape: {"url:...": {url: "..."}}.
    if (key.startsWith('url:') && typeof value.url === 'string') {
      const url = String(value.url).trim();
      if (url.length > 0) {
        urlByKey.set(url, {
          key: url,
          value: {
            url,
            discovered: true,
            isTxt: /\.txt($|[?#])/i.test(url),
          },
        });
      }
      return;
    }

    // Accept reducer output shape for docs.
    if (key.startsWith('doc:') && value.kind === 'doc' && value.docId && value.text) {
      const docId = String(value.docId);
      docByKey.set(docId, {
        key: docId,
        value: {
          docId,
          url: value.url || '',
          text: value.text,
        },
      });
      return;
    }

    // Accept mapper output shape: {"doc:...": {docId, url, text}}.
    if (key.startsWith('doc:') && value.docId && typeof value.text === 'string') {
      const docId = String(value.docId);
      const text = String(value.text).trim();
      if (text.length > 0) {
        docByKey.set(docId, {
          key: docId,
          value: {
            docId,
            url: value.url || '',
            text,
          },
        });
      }
    }
  });

  urlRecords.push(...urlByKey.values());
  docRecords.push(...docByKey.values());

  putAll(distribution, crawlGid, urlRecords, (urlErr) => {
    if (isError(urlErr)) {
      callback(urlErr);
      return;
    }

    putAll(distribution, ridxGid, docRecords, (docErr) => {
      if (isError(docErr)) {
        callback(docErr);
        return;
      }

      callback(null, {
        discoveredUrls: urlRecords.length,
        indexedDocs: docRecords.length,
        urlRecords,
        docRecords,
      });
    });
  });
}

function normalizeSeedRecords(distribution, seeds) {
  return (seeds || []).map((seed) => {
    if (typeof seed === 'string') {
      return {
        key: seed,
        value: {url: seed},
      };
    }
    return {
      key: seed.key || seed.url || `seed:${distribution.util.id.getID(seed)}`,
      value: seed.value || seed,
    };
  });
}

function fetchUrl(url, callback) {
  const requestLib = url.startsWith('https:') ? https : http;
  const req = requestLib.get(url, {timeout: 8000}, (res) => {
    const statusCode = Number(res.statusCode) || 0;

    if (statusCode >= 300 && statusCode < 400 && res.headers.location) {
      const redirected = (() => {
        try {
          return new URL(res.headers.location, url).toString();
        } catch (e) {
          return null;
        }
      })();
      res.resume();
      if (!redirected) {
        callback(null, null);
        return;
      }
      fetchUrl(redirected, callback);
      return;
    }

    if (statusCode < 200 || statusCode >= 300) {
      res.resume();
      callback(null, null);
      return;
    }

    const chunks = [];
    res.on('data', (chunk) => chunks.push(chunk));
    res.on('end', () => callback(null, Buffer.concat(chunks).toString('utf8')));
  });

  req.on('timeout', () => {
    req.destroy();
    callback(null, null);
  });

  req.on('error', () => {
    callback(null, null);
  });
}

function hydrateSeedRecords(seedRecords, callback) {
  if (!Array.isArray(seedRecords) || seedRecords.length === 0) {
    callback(null, []);
    return;
  }

  const hydrated = seedRecords.map((seed) => ({...seed, value: {...(seed.value || {})}}));
  let pending = hydrated.length;

  hydrated.forEach((seed, idx) => {
    const url = seed && seed.value && typeof seed.value.url === 'string' ? seed.value.url : null;
    if (!url || (typeof seed.value.html === 'string' && seed.value.html.length > 0)) {
      pending -= 1;
      if (pending === 0) {
        callback(null, hydrated);
      }
      return;
    }

    fetchUrl(url, (_err, html) => {
      if (typeof html === 'string' && html.length > 0) {
        hydrated[idx].value.html = html;
      }

      pending -= 1;
      if (pending === 0) {
        callback(null, hydrated);
      }
    });
  });
}

function sameHostFilterFactory(seedRecords, crossHostAllowed) {
  if (crossHostAllowed) {
    return () => true;
  }

  const hosts = new Set();
  (seedRecords || []).forEach((seed) => {
    const url = seed && seed.value && seed.value.url;
    if (!url) {
      return;
    }
    try {
      hosts.add(new URL(url).host);
    } catch (e) {
      // Ignore malformed seed URLs.
    }
  });

  return (url) => {
    try {
      return hosts.has(new URL(url).host);
    } catch (e) {
      return false;
    }
  };
}

function persistIndexOutput(distribution, indexOut, rlgGid, callback) {
  const termToPostings = new Map();
  const termRecords = [];

  (indexOut || []).forEach((entry) => {
    if (!entry || typeof entry !== 'object') {
      return;
    }
    const term = Object.keys(entry)[0];
    const value = entry[term];
    if (!term || !value || typeof value !== 'object') {
      return;
    }

    // Reducer output shape: {term: {docId1: count, ...}}.
    if (!Object.prototype.hasOwnProperty.call(value, 'docId')) {
      const merged = termToPostings.get(term) || {};
      Object.entries(value).forEach(([docId, count]) => {
        merged[docId] = (merged[docId] || 0) + (Number(count) || 0);
      });
      termToPostings.set(term, merged);
      return;
    }

    // Mapper output shape fallback: {term: {docId: "...", count: N}}.
    const docId = String(value.docId || '');
    if (!docId) {
      return;
    }
    const merged = termToPostings.get(term) || {};
    merged[docId] = (merged[docId] || 0) + (Number(value.count) || 0);
    termToPostings.set(term, merged);
  });

  termToPostings.forEach((postings, term) => {
    termRecords.push({key: term, value: postings});
  });

  putAll(distribution, rlgGid, termRecords, (e) => {
    if (isError(e)) {
      callback(e);
      return;
    }
    callback(null, {terms: termRecords.length});
  });
}

function runCrawlStage(distribution, options, callback) {
  const crawlGid = options.crawlGid || 'crawl';
  const ridxGid = options.ridxGid || 'ridx';
  const seedRecords = normalizeSeedRecords(distribution, options.seedRecords);

  putAll(distribution, crawlGid, seedRecords, (seedErr) => {
    if (isError(seedErr)) {
      callback(seedErr);
      return;
    }

    const keys = seedRecords.map((seed) => seed.key);
    distribution[crawlGid].mr.exec({keys, map: crawlMap, reduce: crawlReduce}, (e, out) => {
      if (isError(e)) {
        callback(e);
        return;
      }

      persistCrawlOutput(distribution, out, crawlGid, ridxGid, (persistErr, crawlStats) => {
        if (isError(persistErr)) {
          callback(persistErr);
          return;
        }

        if ((crawlStats.discoveredUrls === 0) && (crawlStats.indexedDocs === 0)) {
          const outLen = Array.isArray(out) ? out.length : 0;
          const sample = outLen > 0 ? JSON.stringify(out[0]) : 'none';
          callback(Error(`crawl MR produced no persistable records (outLen=${outLen}, sample=${sample})`));
          return;
        }

        callback(null, crawlStats);
      });
    });
  });
}

function runCrawlWorkflow(distribution, options, callback) {
  options = options || {};
  const maxDepth = Number.isInteger(options.maxDepth) ? options.maxDepth : 1;
  const maxPages = Number.isInteger(options.maxPages) ? options.maxPages : 20;
  const allowCrossHost = options.allowCrossHost === true;

  const initialSeeds = normalizeSeedRecords(distribution, options.seedRecords);
  if (initialSeeds.length === 0) {
    callback(null, {discoveredUrls: 0, indexedDocs: 0, urlRecords: [], docRecords: []});
    return;
  }

  const allowUrl = sameHostFilterFactory(initialSeeds, allowCrossHost);
  const discoveredByUrl = new Map();
  const docsById = new Map();
  const visited = new Set();
  let frontier = initialSeeds;
  let depth = 0;

  function runDepth() {
    if (frontier.length === 0 || depth > maxDepth || visited.size >= maxPages) {
      callback(null, {
        discoveredUrls: discoveredByUrl.size,
        indexedDocs: docsById.size,
        urlRecords: Array.from(discoveredByUrl.values()),
        docRecords: Array.from(docsById.values()),
      });
      return;
    }

    const boundedFrontier = [];
    for (let i = 0; i < frontier.length && visited.size < maxPages; i++) {
      const candidate = frontier[i];
      const url = candidate && candidate.value && candidate.value.url;
      if (!url || visited.has(url) || !allowUrl(url)) {
        continue;
      }
      visited.add(url);
      boundedFrontier.push(candidate);
    }

    if (boundedFrontier.length === 0) {
      callback(null, {
        discoveredUrls: discoveredByUrl.size,
        indexedDocs: docsById.size,
        urlRecords: Array.from(discoveredByUrl.values()),
        docRecords: Array.from(docsById.values()),
      });
      return;
    }

    hydrateSeedRecords(boundedFrontier, (_hydrateErr, hydrated) => {
      runCrawlStage(distribution, {...options, seedRecords: hydrated}, (crawlErr, stats) => {
        if (isError(crawlErr)) {
          callback(crawlErr);
          return;
        }

        (stats.urlRecords || []).forEach((record) => {
          discoveredByUrl.set(record.key, record);
        });
        (stats.docRecords || []).forEach((record) => {
          docsById.set(record.key, record);
        });

        const nextFrontier = [];
        (stats.urlRecords || []).forEach((record) => {
          const url = record && record.value && record.value.url;
          if (!url || visited.has(url) || !allowUrl(url)) {
            return;
          }
          nextFrontier.push({key: url, value: {url}});
        });

        frontier = nextFrontier;
        depth += 1;
        runDepth();
      });
    });
  }

  runDepth();
}

function runIndexStage(distribution, options, callback) {
  const ridxGid = options.ridxGid || 'ridx';
  const rlgGid = options.rlgGid || 'rlg';
  const docRecords = Array.isArray(options.docRecords) ? options.docRecords : [];

  if (docRecords.length === 0) {
    callback(null, {terms: 0});
    return;
  }

  distribution[ridxGid].mr.exec({keys: docRecords.map((record) => record.key), map: indexMap, reduce: indexReduce}, (e, out) => {
    if (isError(e)) {
      callback(e);
      return;
    }

    persistIndexOutput(distribution, out, rlgGid, (persistErr, indexStats) => {
      if (isError(persistErr)) {
        callback(persistErr);
        return;
      }

      if (indexStats.terms === 0) {
        const outLen = Array.isArray(out) ? out.length : 0;
        const sample = outLen > 0 ? JSON.stringify(out[0]) : 'none';
        callback(Error(`index MR produced no term records (outLen=${outLen}, sample=${sample})`));
        return;
      }

      callback(null, indexStats);
    });
  });
}

function runPipeline(distribution, options, callback) {
  options = options || {};
  const useCrawlWorkflow = options.expandCrawl === true;
  const crawlFn = useCrawlWorkflow ? runCrawlWorkflow : runCrawlStage;

  crawlFn(distribution, options, (crawlErr, crawlStats) => {
    if (isError(crawlErr)) {
      callback(crawlErr);
      return;
    }

    runIndexStage(distribution, {...options, docRecords: crawlStats.docRecords || []}, (indexErr, indexStats) => {
      if (isError(indexErr)) {
        callback(indexErr);
        return;
      }

      callback(null, {
        crawl: crawlStats,
        index: indexStats,
      });
    });
  });
}

function enrichRankedWithDocs(distribution, ridxGid, ranked, callback) {
  if (!Array.isArray(ranked) || ranked.length === 0) {
    callback(null, []);
    return;
  }

  if (!distribution[ridxGid] || !distribution[ridxGid].store) {
    callback(null, ranked);
    return;
  }

  const enriched = ranked.map((row) => ({...row}));
  let pending = ranked.length;

  ranked.forEach((row, idx) => {
    distribution[ridxGid].store.get(row.docId, (e, doc) => {
      if (!isError(e) && doc && typeof doc === 'object') {
        if (typeof doc.url === 'string' && doc.url.length > 0) {
          enriched[idx].url = doc.url;
        }
        if (typeof doc.text === 'string' && doc.text.length > 0) {
          enriched[idx].text = doc.text;
        }
      }

      pending -= 1;
      if (pending === 0) {
        callback(null, enriched);
      }
    });
  });
}

function queryIndex(distribution, options, query, callback) {
  const rlgGid = options.rlgGid || 'rlg';
  const ridxGid = options.ridxGid || 'ridx';
  const includeDocs = options.includeDocs !== false;
  const topK = options.topK || 10;
  const terms = String(query || '').toLowerCase().match(/[a-z0-9]+/g) || [];

  if (terms.length === 0) {
    callback(null, []);
    return;
  }

  let pending = terms.length;
  const scores = {};
  let done = false;

  terms.forEach((term) => {
    distribution[rlgGid].store.get(term, (e, postings) => {
      if (done) {
        return;
      }

      if (!isError(e) && postings && typeof postings === 'object') {
        Object.entries(postings).forEach(([docId, count]) => {
          scores[docId] = (scores[docId] || 0) + (Number(count) || 0);
        });
      }

      pending -= 1;
      if (pending === 0) {
        const ranked = Object.entries(scores)
            .map(([docId, score]) => ({docId, score}))
            .sort((a, b) => b.score - a.score)
            .slice(0, topK);
        done = true;
        if (!includeDocs) {
          callback(null, ranked);
          return;
        }
        enrichRankedWithDocs(distribution, ridxGid, ranked, callback);
      }
    });
  });
}

module.exports = {
  crawlMap,
  crawlReduce,
  indexMap,
  indexReduce,
  runCrawlStage,
  runCrawlWorkflow,
  runIndexStage,
  runPipeline,
  queryIndex,
};
