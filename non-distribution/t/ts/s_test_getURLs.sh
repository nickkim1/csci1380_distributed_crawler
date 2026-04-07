#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/..$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}

# This is just a dummy URL, note this is used separately from us parsing the HTML
url="https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/index.html"

# Test whether or not duplicate URLs (hrefs) are output, they should be since
# the visited file is what should be keeping track of dups/cycles
if ! $DIFF <(cat "$T_FOLDER"/d/s1_dupurls.txt | c/getURLs.js $url | sort) <(sort "$T_FOLDER"/d/s1_dupurls_output.txt) >&2;
then
    echo "$0 failure: URL sets are not identical"
    exit 1
fi

echo "$0 success: URL sets are identical"
exit 0
