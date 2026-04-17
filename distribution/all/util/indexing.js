// @ts-check
/**
 * Shared indexing helpers for the distributed crawler/search pipeline.
 */

const { execFileSync } = require("child_process");
const path = require("path");

const processScript = path.join(
  __dirname,
  "../../../non-distribution/c/process.sh",
);
const stemScript = path.join(__dirname, "../../../non-distribution/c/stem.js");

/**
 * Normalize free text into stemmed tokens using the existing M0 scripts.
 * @param {string} text
 * @returns {string[]}
 */
function normalizeTokens(text) {
  if (!text) {
    return [];
  }

  try {
    const processed = execFileSync(processScript, {
      encoding: "utf-8",
      input: text,
      maxBuffer: 10 * 1024 * 1024,
    });
    const stemmed = execFileSync(stemScript, {
      encoding: "utf-8",
      input: processed,
      maxBuffer: 10 * 1024 * 1024,
    });

    return stemmed
      .split(/\r?\n/)
      .map((token) => token.trim())
      .filter(Boolean);
  } catch (error) {
    return [];
  }
}

/**
 * Generate 1..maxN n-grams from a token list.
 * @param {string[]} tokens
 * @param {number} [maxN=3]
 * @returns {string[]}
 */
function generateNGrams(tokens, maxN = 3) {
  const grams = [];
  for (let size = 1; size <= maxN; size++) {
    if (tokens.length < size) {
      continue;
    }
    for (let start = 0; start <= tokens.length - size; start++) {
      grams.push(tokens.slice(start, start + size).join(" "));
    }
  }
  return grams;
}

/**
 * Count term frequencies in a token list.
 * @param {string[]} terms
 * @returns {Map<string, number>}
 */
function countTerms(terms) {
  const counts = new Map();
  for (const term of terms) {
    counts.set(term, (counts.get(term) || 0) + 1);
  }
  return counts;
}

/**
 * Process free text into indexed term counts.
 * @param {string} text
 * @returns {{terms: string[], counts: Map<string, number>, docLength: number}}
 */
function analyzeText(text) {
  const tokens = normalizeTokens(text);
  const terms = generateNGrams(tokens, 3);
  return {
    terms,
    counts: countTerms(terms),
    docLength: terms.length,
  };
}

/**
 * Prepare a query string into the same token stream used for indexing.
 * @param {string[]} parts
 * @returns {{raw: string, tokens: string[], terms: string[]}}
 */
function analyzeQuery(parts) {
  const raw = parts.join(" ").trim();
  const tokens = normalizeTokens(raw);
  const terms = generateNGrams(tokens, 3);
  return { raw, tokens, terms };
}

module.exports = {
  analyzeText,
  analyzeQuery,
  countTerms,
  generateNGrams,
  normalizeTokens,
};
