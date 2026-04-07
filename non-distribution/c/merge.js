#!/usr/bin/env node

/*
Merge the current inverted index (assuming the right structure) with the global index file
Usage: input > ./merge.js global-index > output

The inverted indices have the different structures!

Each line of a local index is formatted as:
  - `<word/ngram> | <frequency> | <url>`

Each line of a global index is be formatted as:
  - `<word/ngram> | <url_1> <frequency_1> <url_2> <frequency_2> ... <url_n> <frequency_n>`
  - Where pairs of `url` and `frequency` are in descending order of frequency
  - Everything after `|` is space-separated

-------------------------------------------------------------------------------------
Example:

local index:
  word1 word2 | 8 | url1
  word3 | 1 | url9
EXISTING global index:
  word1 word2 | url4 2
  word3 | url3 2

merge into the NEW global index:
  word1 word2 | url1 8 url4 2
  word3 | url3 2 url9 1

Remember to error gracefully, particularly when reading the global index file.
*/

const fs = require('fs');
const readline = require('readline');
// The `compare` function can be used for sorting.
const compare = (a, b) => {
  if (a.freq > b.freq) {
    return -1;
  } else if (a.freq < b.freq) {
    return 1;
  } else {
    return 0;
  }
};
const rl = readline.createInterface({
  input: process.stdin,
});

// 1. Read the incoming local index data from standard input (stdin) line by line.
let localIndex = '';
rl.on('line', (line) => {
  localIndex += line + '\n';
});

rl.on('close', () => {
  // 2. Read the global index name/location, using process.argv
  // and call printMerged as a callback
  const filePath = process.argv[2];
  // Need to set the type otherwise not parsed as a string, instead parsed as a buffer
  fs.readFile(filePath, 'utf8', printMerged);
});

const printMerged = (err, data) => {
  if (err) {
    console.error('Error reading file:', err);
    return;
  }

  // Split the data into an array of lines
  const localIndexLines = localIndex.split('\n');
  const globalIndexLines = data.split('\n');

  localIndexLines.pop();
  globalIndexLines.pop();

  const local = new Map();
  const global = new Map();

  // 3. For each line in `localIndexLines`, parse them and add them to the `local` object
  // where keys are terms and values store a url->freq map (one entry per url).
  for (const line of localIndexLines) {
    let [term, ...values] = line.split('|');
    term = term.trim();

    if (!local.has(term)) {
      local.set(term, new Map());
    }
    local.get(term).set(values[1].trim(), Number(values[0]));
  }

  // 4. For each line in `globalIndexLines`, parse them and add them to the `global` object
  // where keys are terms and values are url->freq maps (one entry per url).
  // Use the .trim() method to remove leading and trailing whitespace from a string.
  for (const line of globalIndexLines) {
    let [term, values] = line.split('|');
    term = term.trim();
    // Split on arb. number of whitespaces since its formatted like |_https... 2_ (get rid of underscored whitespace)
    const splitVals = values.trim().split(/\s+/);

    const grouped = new Map();
    for (let i = 0; i < splitVals.length; i += 2) {
      grouped.set(splitVals[i].trim(), Number(splitVals[i + 1]));
    }
    global.set(term, grouped); // Map<url, freq>
  }

  // 5. Merge the local index into the global index:
  // - For each term in the local index, if the term exists in the global index:
  //     - Merge by url so there is at most one entry per url.
  //     - Sum frequencies for duplicate urls.
  // - If the term does not exist in the global index:
  //     - Add it as a new entry with the local index's data.
  // 6. Print the merged index to the console in the same format as the global index file:
  //    - Each line contains a term, followed by a pipe (`|`), followed by space-separated pairs of `url` and `freq`.
  //    - Terms should be printed in alphabetical order.
  for (let [term, urlMap] of local) {
    term = term.trim();
    if (global.has(term)) {
      for (const [url, freq] of urlMap) {
        if (global.get(term).has(url)) {
          global.get(term).set(url, global.get(term).get(url) + freq);
        } else {
          global.get(term).set(url, freq);
        }
      }
    } else {
      global.set(term, urlMap);
    }
  }

  const sortedTerms = Array.from(global.keys()).sort();
  for (const term of sortedTerms) {
    const sortedUrlsFreqs = Array.from(global.get(term).entries())
        .map(([url, freq]) => ({
          'url': url,
          'freq': freq,
        }))
        .sort(compare)
        .map((obj) => `${obj.url} ${obj.freq}`)
        .join(' ');
    console.log(`${term} | ${sortedUrlsFreqs}`);
  }
};
