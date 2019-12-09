#!/bin/sh

# set local environment variables
PROJECT='book-recommendations-259715 '
BUCKET_NAME='bookreview_bucket'

# chunk the full dataset if it has been generated
mkdir -p chunks/processed
split --bytes 500M --numeric-suffixes --suffix-length=4 \
        --additional-suffix=.json processed.json \
        processed_
mv processed_0*.json chunks/processed/
# rm -r chunks/metabooks chunks/reviews