#!/bin/sh

# set local environment variables
PROJECT='book-recommendations-259715 '
BUCKET_NAME='bookreview_bucket'

# chunk the full dataset if it has been generated
mkdir -p gs://${BUCKET_NAME}/chunks/processed
split --bytes 500M --numeric-suffixes --suffix-length=4 \
        --additional-suffix=.json gs://${BUCKET_NAME}/processed.json \
        processed_
mv gs://${BUCKET_NAME}/processed_0*.json gs://${BUCKET_NAME}/chunks/processed/
    # rm -r chunks/metabooks chunks/reviews