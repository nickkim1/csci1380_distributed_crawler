#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/..$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}


if $DIFF <(cat "$T_FOLDER"/d/s2_stemmed.txt | c/stem.js | sort) <(sort "$T_FOLDER"/d/s2_stemmed_output.txt) >&2;
then
    echo "$0 success: stemmed words are identical"
    exit 0
else
    echo "$0 failure: stemmed words are not identical"
    exit 1
fi
