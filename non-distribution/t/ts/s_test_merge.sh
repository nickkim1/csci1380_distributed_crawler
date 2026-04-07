#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/..$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}
DIFF_PERCENT=${DIFF_PERCENT:-0}

cat /dev/null > d/global-index.txt

files=("$T_FOLDER"/d/s4_idx{1..2}_nooverlap.txt)

for file in "${files[@]}"
do
    cat "$file" | c/merge.js d/global-index.txt > d/temp-global-index.txt
    mv d/temp-global-index.txt d/global-index.txt
done

EXIT=0

# Tests for merging two indices with no overlap
if DIFF_PERCENT=$DIFF_PERCENT t/gi-diff.js <(sort d/global-index.txt) <(sort "$T_FOLDER"/d/s4_idx3_nooverlap.txt) >&2;
then
    echo "$0 success: global indexes are identical"
else
    echo "$0 failure: global indexes are not identical"
    EXIT=1
fi

# Tests for merging identical indices (test idempotency)
cat /dev/null > d/global-index.txt
cat "$T_FOLDER"/d/s4_idx_identical.txt | c/merge.js d/global-index.txt > d/temp-global-index.txt
cat "$T_FOLDER"/d/s4_idx_identical.txt | c/merge.js d/global-index.txt > d/temp-global-index.txt
mv d/temp-global-index.txt d/global-index.txt

if DIFF_PERCENT=$DIFF_PERCENT t/gi-diff.js <(sort d/global-index.txt) <(sort "$T_FOLDER"/d/s4_idx_identical_output.txt) >&2;
then
    echo "$0 success: global indexes are identical"
else
    echo "$0 failure: global indexes are not identical"
    EXIT=1
fi

exit $EXIT