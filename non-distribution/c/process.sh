#!/bin/bash

# Convert input to a stream of non-stopword terms
# Usage: input > ./process.sh > output

# Convert non-letter characters to newlines, make lowercase, convert to ASCII; then remove stopwords (inside d/stopwords.txt)
# Non-letter characters include things like ©, ®, and ™ as well!

# Commands that will be useful: tr, iconv, grep
tr -cs 'a-zA-Z' '\n' | tr '[:upper:]' '[:lower:]' | iconv -t ASCII | grep -v -w -f 'd/stopwords.txt' || true
# * The bug I was getting w/ filenotfound was b/c I was using
# input redirection into the first tr command (<$1)

# Tip: Make sure your program doesn't emit a non-zero exit code if there are no words left after removing stopwords.
# You can combine the grep invocation with `|| true` to achieve this. Be careful though, as this will also hide other errors!