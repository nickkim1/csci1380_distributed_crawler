# Distributed Crawl, Index, and Query Service

A distributed web crawling and search pipeline built on the M5 MapReduce framework. The system spreads crawling load across multiple nodes, builds a sharded inverted index, and serves ranked queries from distributed storage.

## Architecture

### Core Components

1. **Main Crawler Service** (`distribution/all/crawler.js`)
   - Orchestrates round-based crawling using MapReduce
   - Manages state transitions and completion detection
   - Coordinates mapper/reducer execution across nodes

2. **URL Utilities** (`distribution/all/util/crawler-url-utils.js`)
   - Canonicalization for deduplication
   - Trap detection (session URLs, parameter explosion, etc.)
   - Domain extraction and crawl target validation

3. **Frontier Management** (`distribution/all/util/crawler-frontier.js`)
   - Persistent frontier queue in distributed store
   - Visited URL set tracking
   - Round-to-round state updates

4. **State Inspection** (`distribution/all/util/crawler-state.js`)
   - Cycle detection
   - Pathological case analysis (traps, light-content corners)
   - Diagnostic reporting

5. **Distributed Indexer** (`distribution/all/indexer.js`)

- Reads crawled documents from persistent storage
- Normalizes text into terms and n-grams
- Merges postings into a sharded inverted index

6. **Distributed Query Service** (`distribution/all/query.js`)

- Reads term postings from the distributed index
- Scores documents with TF-IDF-style ranking
- Returns optional debug information for relevance analysis

7. **Reference Implementation** (`distribution/all/crawler-reference.js`)
   - Complete mapper/reducer implementations
   - HTML parsing and URL extraction
   - Distributed indexing pipeline

## Load Distribution Strategy

### Round-Based Execution

Each crawl round follows this pattern:

```
                     ┌──────────────────────┐
                     │  Current Frontier    │
                     │ (from store)         │
                     └──────────┬───────────┘
                                │
                    ┌───────────▼────────────┐
                    │  Linear Partitioning   │
                    │  URLs → Nodes (hash)   │
                    └───────────┬────────────┘
                                │
                    ┌───────────▼────────────┐
      ┌─────────────┤  MAP PHASE             │
      │             │  Each node crawls      │
      │             │  its assigned URLs     │
      │             └────────┬───────────────┘
      │                      │
      │             ┌────────▼───────────┐
      │             │  Mapper outputs:    │
      │             │  {url, outlinks,    │
      │             │   text, failed}     │
      │             └────────┬───────────┘
      │                      │
      │             ┌────────▼───────────┐
      └────────────▶│  SHUFFLE PHASE      │
                    │  Group by URL key   │
                    │  (via mem.append)   │
                    └────────┬───────────┘
                             │
                    ┌────────▼───────────┐
                    │  REDUCE PHASE       │
                    │  Deduplicate URLs   │
                    │  Filter visited     │
                    │  Build next frontier│
                    └────────┬───────────┘
                             │
                    ┌────────▼───────────┐
                    │  Update Store       │
                    │  - visited set      │
                    │  - next frontier    │
                    │  - crawl results    │
                    └────────┬───────────┘
                             │
                        ┌────▼────┐
                        │ Next Round
                        └──────────┘
```

### Key Partitioning

URLs are distributed deterministically using hash-based partitioning:

```javascript
// Linear partitioning across nodes
keysPerNode = Math.ceil(urls.length / nodeCount);
nodeIndex = hash(url) % nodeCount;
assignedNode = nodes[nodeIndex];
```

Benefits:

- Deterministic: same URL always goes to same node (enables local caching)
- Balanced: even distribution across nodes
- Scalable: adding nodes redistributes load automatically

## Indexing Design

The indexer consumes crawled document records and builds a persistent inverted index in the distributed store.

### Document Flow

1. The crawler stores one document record per visited URL.
2. The indexer reads those stored documents by crawl ID or explicit key list.
3. Each document is normalized into stemmed tokens and short n-grams.
4. The MapReduce job emits term postings keyed by term.
5. The reducer merges postings for the same term and preserves document frequency.
6. The merged records are written back to shard-local storage.

### Stored Index Data

The index is stored as term records plus aggregate metadata:

```javascript
__meta__ = {
  docCount: number,
  termCount: number,
  updatedAt: number,
};

term = {
  term: string,
  postings: [
    {
      url: string,
      tf: number,
      docLen: number,
      title: string,
    },
  ],
  df: number,
};
```

### Index Partitioning

Index terms are stored in the distributed store abstraction used by the rest of the system, so queries can fetch the term shard directly by key. This keeps index lookup local to the responsible node and avoids a central index server.

### Ranking Inputs

The indexer preserves the features needed for ranking:

- term frequency per document
- document length
- document frequency per term
- page title when available

These values are enough for TF-IDF-style scoring and phrase-sensitive weighting.

## Query Design

The query service consumes a user query, normalizes it with the same text pipeline as indexing, fetches matching term records, and computes ranked results.

### Query Flow

1. Normalize the query text into the same token stream used during indexing.
2. Deduplicate the query terms.
3. Read index metadata for corpus size.
4. Fetch postings for each query term from the distributed store.
5. Score each candidate document using term frequency, document frequency, and document length.
6. Sort by score and return the top results.

### Scoring Model

The current implementation uses a TF-IDF-style score with a length normalization factor:

$$
	ext{score}(d, q) = \sum_{t \in q} \left(\frac{tf_{t,d}}{\sqrt{len(d)}}\right) \cdot \left(\log\frac{N + 1}{df_t + 1} + 1\right) \cdot w(t)
$$

Where:

- $tf_{t,d}$ is the term frequency of term $t$ in document $d$
- $len(d)$ is the tokenized document length
- $df_t$ is the document frequency of term $t$
- $N$ is the corpus size
- $w(t)$ is a small phrase bonus for multi-word terms

### Query Output

The query service returns:

```javascript
[
  {
    url: string,
    score: number,
    title?: string,
    features?: {
      terms: {
        [term: string]: {
          tf: number,
          df: number,
          idf: number,
          contribution: number
        }
      },
      tfidf: number,
      phraseBonus: number,
      lengthNorm: number
    }
  }
]
```

The optional `features` payload is meant for debugging and relevance inspection.

## Usage

### Basic Example

```javascript
const distribution = require("@brown-ds/distribution")();

distribution.node.start(() => {
  // Spawn additional worker nodes
  distribution.local.status.spawn({ ip: "127.0.0.1", port: 8081 }, (err) => {
    // Start crawling
    distribution.all.crawler.exec(
      {
        seedURLs: [
          "https://atlas.cs.brown.edu/data/gutenberg/",
          "https://example.com",
        ],
        maxDepth: 3,
        maxPages: 100000,
        gid: "all",
      },
      (err, results) => {
        if (err) {
          console.error("Crawl failed:", err);
          return;
        }
        console.log("Crawl complete:", results);
        // {
        //   crawlID: '...',
        //   stats: {
        //     totalPagesVisited: 50000,
        //     totalURLsDiscovered: 150000,
        //     failedURLs: 25,
        //     duplicatesFiltered: 125000,
        //     roundsCompleted: 4,
        //     cyclesDetected: 0
        //   }
        // }
      },
    );
  });
});
```

### Configuration Options

```javascript
{
  seedURLs: [],           // Required: Initial URLs
  maxDepth: 3,           // Max crawl depth (default: 3)
  maxPages: 100000,      // Max pages to crawl (default: 100K)
  gid: 'all',           // Group ID for nodes (default: 'all')
  timeout: 30000,       // Per-URL timeout in ms (default: 30s)
  maxConcurrentPerNode: 10  // Worker concurrency (default: 10)
}
```

## State Management

### Persistent Storage

All crawler state is stored in `distribution.all.store`:

```javascript
// Visited URL set
crawler:${crawlID}:visited = {
  urls: Set<string>,
  count: number
}

// Frontier queue
crawler:${crawlID}:frontier = {
  entries: [
    {
      url: string,
      depth: number,
      discoveredFrom: string,  // 'seed' | 'crawl'
      retryCount: number
    }
  ]
}

// Crawl results per URL
crawl_result:${crawlID}:${url} = {
  url: string,
  outlinks: string[],
  text: string,
  title: string,
  contentLength: number,
  headers: Object,
  timestamp: number,
  failed: boolean,
  error?: string
}
```

### Local Node Caching

Each worker node maintains a local cache of crawled URLs to avoid redundant fetching within the same round.

## Pathological Case Detection

The system automatically detects and reports:

### 1. **Cycles and Stalled Crawls**

- Detected when: visited count doesn't increase and frontier shrinks
- Action: Continue until frontier empty or max pages reached

### 2. **Session-Based URL Traps**

- Patterns: JSESSIONID, PHPSESSID, temp directories
- Detected: Before fetching via URL pattern matching
- Action: Skip URL and continue

### 3. **Parameter Explosion**

- Pattern: URLs with >15 query parameters
- Detected: Before fetching
- Action: Skip and log as trap

### 4. **Excessive Nesting**

- Pattern: Paths with >10 slash separators
- Detected: Before fetching
- Action: Skip as potential infinite directory structure

### 5. **Duplicate Content**

- Detected: Post-crawl via content hash comparison
- Metric: Reported in diagnostics
- Action: Logged as warning if >15% of pages

### 6. **Low-Content Domains**

- Detected: Post-round analysis
- Definition: Domains with <3 pages discovered
- Action: May deprioritize in next round

### 7. **High Failure Rates**

- Threshold: >30% of URLs fail to fetch
- Action: Reported as alert, crawl continues with degraded status

## Monitoring and Diagnostics

### Get Crawler State

```javascript
distribution.all.crawler.state((err, state) => {
  console.log(state);
  // {
  //   coordinator: 'node-id',
  //   nodeCount: 3,
  //   nodes: { ... },
  //   timestamp: 1234567890
  // }
});
```

### Cycle Detection

```javascript
const cycle = stateInspector.detectCycle(prevMetrics, currMetrics);
if (cycle) {
  console.log(`Detected ${cycle.type}:`, cycle);
  // {
  //   type: 'stalled_crawl',
  //   prevVisited: 100,
  //   currVisited: 100,
  //   prevFrontier: 50,
  //   currFrontier: 30,
  //   severity: 'ongoing'
  // }
}
```

### Trap Analysis

```javascript
const analysis = stateInspector.analyzeTrapIndicators(crawlResults);
console.log(analysis);
// {
//   sessionURLs: 5,
//   dynamicGenerated: 8,
//   infiniteDepth: 0,
//   duplicateContent: 3,
//   failureRate: 0.02
// }
```

### Diagnostic Report

```javascript
const report = stateInspector.generateDiagnostics(stats, metrics, trapAnalysis);
console.log(report);
// {
//   status: 'healthy' | 'degraded' | 'error',
//   alerts: [],
//   warnings: [],
//   metrics: { ... },
//   trapAnalysis: { ... }
// }
```

## Persistence and Recovery

The crawler maintains full state in distributed storage, enabling stop/resume:

```javascript
// Stop current crawl (state is persisted)
// ... later ...

// Resume from same point
distribution.all.crawler.exec({
  crawlID: 'previous-crawl-id',  // Optional: resume existing crawl
  seedURLs: [...],
  // ... rest of config
}, callback);
```

## Performance Optimization

### 1. **Local Caching**

- Each node caches recently crawled URLs in memory
- Avoids network overhead for duplicate requests within same round

### 2. **Early Filtering**

- Trap detection happens before fetch (saves network I/O)
- Content-type validation before parsing

### 3. **Sharding by URL Hash**

- Deterministic partitioning ensures:
  - Balanced load across nodes
  - Cache locality (same URL always on same node)
  - Scalable addition of new nodes

### 4. **Lazy Frontier Update**

- Only update frontier after entire round completes
- Batch operations to reduce store writes

## Testing

Run the crawler test suite:

```bash
npm test -- m5.crawler.all.test.js
```

Tests cover:

- URL canonicalization (10 pts)
- Trap detection (10 pts)
- Crawl target validation (10 pts)
- Domain extraction (10 pts)
- Cycle detection (10 pts)
- Trap analysis (10 pts)
- Diagnostic generation (10 pts)

## File Structure

```
distribution/all/
├── crawler.js                    # Main service
├── crawler-reference.js          # Reference implementations
└── util/
    ├── crawler-url-utils.js     # URL utilities
    ├── crawler-frontier.js      # Frontier management
    └── crawler-state.js         # State inspection
test/
└── m5.crawler.all.test.js       # Test suite
```

## Current Implementation Notes

The crawler, indexer, and query service are implemented as separate services under `distribution/all/`.

- The crawler keeps the frontier, seen set, and document manifests minimal.
- The indexer builds and merges term postings from stored crawl documents.
- The query service reads the index, ranks matching documents, and can return explanations.

## Next Steps for Full Implementation

1. **Improve crawl robustness**

- Add explicit timeout handling for page fetches
- Handle fetch retries and transient failures more carefully
- Preserve redirect metadata when useful for indexing

2. **Improve index quality**

- Add better stopword handling and field weighting
- Consider storing link-based signals for ranking
- Expand metadata captured from crawled pages

3. **Improve query quality**

- Add query-time phrase boosting and tie-breakers
- Support filtering by domain or crawl batch
- Add richer explanation output for ranked results

4. **Add persistence refinements**

- Serialize crawl and index snapshots for recovery
- Support incremental updates across multiple crawl runs
- Keep manifests compact as the corpus grows

5. **Advanced scheduling**

- Priority frontier (depth-first vs breadth-first)
- Politeness delays per domain
- Adaptive batch sizing based on node capacity

6. **Cross-service integration**

- Pipe crawl output into indexing automatically
- Route query requests against the active index shard set
- Expose a small CLI or RPC wrapper for end-to-end use
