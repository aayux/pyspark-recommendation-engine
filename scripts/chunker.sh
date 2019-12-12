#!/bin/sh

# chunk the full dataset if it has been generated
split --bytes 500M --numeric-suffixes --suffix-length=4 \
      --additional-suffix=.json processed.json \
      processed_