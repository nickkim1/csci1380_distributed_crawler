#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/../../$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}

EXIT=0

if $DIFF <(cat "$T_FOLDER"/d/s7_singleterm.txt | c/combine.sh | sed 's/\t*$//' | sed 's/\s/ /g' | sort | uniq) <(cat "$T_FOLDER"/d/s7_singleterm.txt | sed 's/\t*$//' | sed 's/\s/ /g' | sort | uniq) >&2;
then
    echo "$0 success: ngrams are identical"
else
    echo "$0 failure: ngrams are not identical"
    EXIT=1
fi

if $DIFF <(cat "$T_FOLDER"/d/s7_bigram.txt | c/combine.sh | sed 's/\t*$//' | sed 's/\s/ /g' | sort | uniq) <(cat "$T_FOLDER"/d/s7_bigram_output.txt | sed 's/\t*$//' | sed 's/\s/ /g' | sort | uniq) >&2;
then
    echo "$0 success: ngrams are identical"
else
    echo "$0 failure: ngrams are not identical"
    EXIT=1
fi

exit $EXIT