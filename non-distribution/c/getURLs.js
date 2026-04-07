#!/usr/bin/env node

/*
Extract all URLs from a web page.
Usage: page.html > ./getURLs.js <base_url>
*/

const readline = require('readline');
const {JSDOM} = require('jsdom');
const {URL} = require('url');

// 1. Read the base URL from the command-line argument using `process.argv`.
let baseURL = process.argv[2];

if (baseURL.endsWith('index.html')) {
  baseURL = baseURL.slice(0, baseURL.length - 'index.html'.length);
} else {
  baseURL += '/';
}

const rl = readline.createInterface({
  input: process.stdin,
});

let htmlInput = '';

rl.on('line', (line) => {
  // 2. Read HTML input from standard input (stdin) line by line using the `readline` module.
  htmlInput += line + '\n';
});

rl.on('close', () => {
  // 3. Parse HTML using jsdom
  const dom = new JSDOM(htmlInput);

  // 4. Find all URLs:
  //  - select all anchor (`<a>`) elements) with an `href` attribute using `querySelectorAll`.
  //  - extract the value of the `href` attribute for each anchor element.
  const els = dom.window.document.querySelectorAll('a[href]');
  const hrefs = Array.from(els).map((el) => el.getAttribute('href'));
  // ** forEach doesn't return a nodelist, it returns undefined

  // 5. Print each absolute URL to the console, one per line.
  hrefs.forEach((href) => {
    const urlObject = new URL(href, baseURL);
    console.log(urlObject.href);
  });
});


