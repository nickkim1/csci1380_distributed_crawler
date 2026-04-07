# M0: Setup & Centralized Computing

> Add your contact information below and in `package.json`.

* name: `Nicolas Kim`

* email: `nicolas_kim@brown.edu`

* cslogin: `nhkim`


## Summary

> Summarize your implementation, including the most challenging aspects; remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete M0 (`hours`), the total number of JavaScript lines you added, including tests (`jsloc`), the total number of shell lines you added, including for deployment and testing (`sloc`).


My implementation consists of 6 components addressing T1--8. The most challenging aspect was debugging my merge implementation because I was unfamiliar with many JS errors that were thrown back (linter and otherwise), and it was a constant back-and-forth to implement. The same went for my process.sh implementation, because some errors (like a file not found resulting from accidentally streaming into tr using file input vs. stdin) took a bit to understand and debug.


## Correctness & Performance Characterization


> Describe how you characterized the correctness and performance of your implementation.

To characterize correctness, we developed 15 tests that test the following cases:
1. combine: a single word, also just two words. This tests singleton and bigram creation, not a mix.
2. getText: on an empty file. 
3. getURLs: on a file with duplicate URLs.
4. invert: on terms with no internal repeats.
5. merge: merging two indices with no overlap, merging the same index (i.e., identical indices).
6. process: processing only stopwords, processing only non-stopword processable words (i.e., capitalized, non-letter)
7. query: query for a non-existent term, query up to two terms, query up to three terms.
8. stem: only non-stemmable words.

*Performance*: The throughput of various subsystems is described in the `"throughput"` portion of package.json. The characteristics of my development machines (here, a M3 Macbook Pro) are summarized in the `"dev"` portion of package.json. Notably the throughput on the instance vs. locally was much lower across the board for the crawler, indexer, and query subsystems for the same inputs. The end to end student test (see non-distribution/t/ts/s_test_end_to_end.sh) was used to characterize throughput on both machines, running the crawler, indexer, and query on sandbox 4 (228 unique URLs). Of note is that I added minor changes to crawl.sh to prevent the crawler from re-traversing the same URLs, since I ran into an issue where duplicate URLs are present in url.txt, adding some overhead (and consequently lowering the throughput).

## Wild Guess

> How many lines of code do you think it will take to build the fully distributed, scalable version of your search engine? Add that number to the `"dloc"` portion of package.json, and justify your answer below.

I think with everything combined - tests, implementation, etc. - it will take roughly 3000 lines of code to build the search engine. If it took me a few hundred lines of code to implement some small JS/Bash scripts in M0, I think that for the remaining 6 milestones it's reasonable to expect at least that number of lines. If you multiply that out, accounting also for the fact that we're going to deal with implementing more complex ideas (e.g., hashing), I think it is reasonable to arrive at a few thousand lines of code especially given the tests we will also be writing.

# non-distribution

This milestone aims (among others) to refresh (and confirm) everyone's
background on developing systems in the languages and libraries used in this
course.

By the end of this assignment you will be familiar with the basics of
JavaScript, shell scripting, stream processing, Docker containers, deployment
to AWS, and performance characterization—all of which will be useful for the
rest of the project.

Your task is to implement a simple search engine that crawls a set of web
pages, indexes them, and allows users to query the index. All the components
will run on a single machine.

## Getting Started

To get started with this milestone, run `npm install` inside this folder. To
execute the (initially unimplemented) crawler run `./engine.sh`. Use
`./query.js` to query the produced index. To run tests, do `npm run test`.
Initially, these will fail.

### Overview

The code inside `non-distribution` is organized as follows:

```
.
├── c            # The components of your search engine
├── d            # Data files like seed urls and the produced index
├── s            # Utility scripts for linting your solutions
├── t            # Tests for your search engine
├── README.md    # This file
├── crawl.sh     # The crawler
├── index.sh     # The indexer
├── engine.sh    # The orchestrator script that runs the crawler and the indexer
├── package.json # The npm package file that holds information like JavaScript dependencies
└── query.js     # The script you can use to query the produced global index
```

### Submitting

To submit your solution, run `./scripts/submit.sh` from the root of the stencil. This will create a
`submission.zip` file which you can upload to the autograder.