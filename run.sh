#!/bin/sh

# set local environment variables
PROJECT=<project-id>
BUCKET_NAME=<bucket-name>
CLUSTER=<cluster-name>
ZONE="us-west1-a"

# create the working directories and chunk the data if not already chunked
if ! [ -d gs://${BUCKET_NAME}/chunks/reviews ]; then
    mkdir -p gs://${BUCKET_NAME}/chunks/reviews
    split --bytes 500M --numeric-suffixes --suffix-length=3 \
          --additional-suffix=.json gs://${BUCKET_NAME}/review_Books_5.json \
          reviews_
    mv gs://${BUCKET_NAME}/reviews_0*.json gs://${BUCKET_NAME}/chunks/reviews/
    # rm review_Books_5.json
fi

if ! [ -d gs://${BUCKET_NAME}/chunks/metabooks ]; then
    mkdir -p gs://${BUCKET_NAME}/chunks/metabooks
    split --bytes 500M --numeric-suffixes --suffix-length=3 \
          --additional-suffix=.json gs://${BUCKET_NAME}/metaBooks.json \
          metabooks_
    mv gs://${BUCKET_NAME}/metabooks_0*.json gs://${BUCKET_NAME}/chunks/metabooks/
    # rm metaBooks.json
fi

# chunk the full dataset if it has been generated
if [ -f gs://${BUCKET_NAME}/data.json ] && \
 ! [ -d gs://${BUCKET_NAME}/chunks/processed ]; then
    mkdir -p gs://${BUCKET_NAME}/chunks/processed
    split --bytes 500M --numeric-suffixes --suffix-length=4 \
          --additional-suffix=.json gs://${BUCKET_NAME}/processed.json \
          processed_
    mv gs://${BUCKET_NAME}/processed_0*.json gs://${BUCKET_NAME}/chunks/processed/
    # rm -r chunks/metabooks chunks/reviews
fi

# create a cloud Dataproc cluster
gcloud dataproc clusters create $CLUSTER \
    --project=${PROJECT} \
    --zone=${ZONE} \
    --single-node

# submit job to Cloud Dataproc cluster
gcloud dataproc jobs submit pyspark main.py \
    --cluster=${CLUSTER} \
    -- gs://${BUCKET_NAME}/

# delete the cloud Dataproc cluster
gcloud dataproc clusters delete $CLUSTER

# delete the Dataproc bucket
# TO DO: find how to get bucket name
# gsutil rb [BUCKET_NAME]