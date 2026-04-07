#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/..$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}

EXIT=0

# Processes stream with only stopwords
if $DIFF <(cat "$T_FOLDER"/d/s3_allstopwords.txt | c/process.sh | sort) <(sort "$T_FOLDER"/d/s3_allstopwords_output.txt) >&2;
then
    echo "$0 success: texts are identical"
else
    echo "$0 failure: texts are not identical"
    EXIT=1
fi

# Processes stream with all terms being processable but not being stopwords
if $DIFF <(cat "$T_FOLDER"/d/s3_nostopwords.txt | c/process.sh | sort) <(sort "$T_FOLDER"/d/s3_nostopwords_output.txt) >&2;
then
    echo "$0 success: texts are identical"
else
    echo "$0 failure: texts are not identical"
    EXIT=1
fi

exit $EXIT
