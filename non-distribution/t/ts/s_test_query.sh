#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/..$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}

cat "$T_FOLDER"/d/m4.txt > d/global-index.txt

EXIT=0

# Tests for querying for a non-existent term in the index, i.e., no query results
if $DIFF <(./query.js "bleh") <(cat "$T_FOLDER"/d/s5_noqueryresults.txt) >&2;
then
    echo "$0 success: search results are identical"
else
    echo "$0 failure: search results are not identical"
    EXIT=1
fi

# Tests for the entire index getting returned, implicitly also for bigram detection
cat "$T_FOLDER"/d/s6_allqueryresults.txt > d/global-index.txt

if $DIFF <(./query.js "check" "stuff") <(cat "$T_FOLDER"/d/s6_allqueryresults.txt) >&2;
then
    echo "$0 success: search results are identical"
else
    echo "$0 failure: search results are not identical"
    EXIT=1
fi

# Test for up to trigram query
if $DIFF <(./query.js "check" "stuff" "dude") <(cat "$T_FOLDER"/d/s6_allqueryresults.txt) >&2;
then
    echo "$0 success: search results are identical"
else
    echo "$0 failure: search results are not identical"
    EXIT=1
fi

exit $EXIT