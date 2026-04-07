#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/..$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}

# Tests HTML files with no content and diffs against an empty file
if $DIFF <(cat "$T_FOLDER"/d/s0_notext.txt | c/getText.js | sort) <(sort "$T_FOLDER"/d/s0_notext_output.txt) >&2;
then
    echo "$0 success: texts are identical"
    exit 0
else
    echo "$0 failure: texts are not identical"
    exit 1
fi
