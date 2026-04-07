#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/../../$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}
DIFF_PERCENT=${DIFF_PERCENT:-0}

cat /dev/null > d/visited.txt
cat /dev/null > d/global-index.txt

# cat "$T_FOLDER"/d/u.txt > d/urls.txt
echo https://cs.brown.edu/courses/csci1380/sandbox/4 > d/urls.txt

# Performance Tests
# runs on the same url set as the default, but provides throughput estimations for each
INDEX_TIME=0
CRAWLER_TIME=0

# Index and crawl for a set number of URLs
while read -r url; do

  if [[ "$url" == "stop" ]]; then 
    exit;
  fi

  echo "[engine] crawling $url">/dev/stderr
  crawl_start=$(date +%s)
  ./crawl.sh "$url" >d/content.txt
  crawl_end=$(date +%s)
  CRAWLER_TIME=$((CRAWLER_TIME + (crawl_end - crawl_start)))

  echo "[engine] indexing $url">/dev/stderr
  index_start=$(date +%s)
  ./index.sh d/content.txt "$url"
  index_end=$(date +%s)
  INDEX_TIME=$((INDEX_TIME + (index_end - index_start)))

  NUM_URLS=$(wc -l < d/visited.txt)
  if  [[ "$(cat d/visited.txt | wc -l)" -ge "$(cat d/urls.txt | wc -l)" ]]; then
      echo "Crawler throughput (urls/s): $((NUM_URLS))/$((CRAWLER_TIME))"
      echo "Index throughput (pg/s): $((NUM_URLS))/$((INDEX_TIME)))"
      break;
  fi

# the -f option: output appended data (urls) as the (url) file grows
# feed each line to the while loop
done < <(tail -f d/urls.txt)

# Query the system for a set number of queries after that's all done
cat "$T_FOLDER"/d/d7.txt > d/global-index.txt

QUERY_TIME=0
# declare -a query_terms=("stuff" "right" "simpl")
declare -a query_terms=("abolitionist" "accept" "baldwin" "read" "publish")

# Loop through each element of the array
for term in "${query_terms[@]}"; do
    query_start=$(date +%s)
    ./query.js "$term"
    query_end=$(date +%s)
    QUERY_TIME=$((QUERY_TIME + (query_end - query_start)))
done

echo "Query throughput (queries/s): $((5))/$((QUERY_TIME)))"