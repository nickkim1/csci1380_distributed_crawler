#!/bin/bash

tmp_file="$(mktemp d/crawl.XXXXXX)"
tmp_urls="$(mktemp d/url.XXXXXX)"

if ! curl -skL --retry 3 --retry-delay 1 --retry-connrefused "$1" >"$tmp_file"; then
  rm -f "$tmp_file"
  exit 1
fi

echo "$1" >>d/visited.txt

# ** Had to add an additional grep to avoid cycles
c/getURLs.js "$1" <"$tmp_file" | grep -vxf d/visited.txt | grep -vxf d/urls.txt > "$tmp_urls"
cat "$tmp_urls" >> d/urls.txt
rm -f "$tmp_urls"
# c/getURLs.js "$1" <"$tmp_file" | grep -vxf d/visited.txt >> d/urls.txt
c/getText.js <"$tmp_file"

rm -f "$tmp_file"
