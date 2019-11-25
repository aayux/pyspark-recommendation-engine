#!/bin/sh

# set local environment variables
PROJECT=<project-id>
BUCKET_NAME=<bucket-name>
CLUSTER=<cluster-name>
ZONE="us-west1-a"


# create a cloud Dataproc cluster
gcloud dataproc clusters create $CLUSTER \
    --project=${PROJECT} \
    --zone=${ZONE} \
    --single-node

# submit job to Cloud Dataproc cluster
gcloud dataproc jobs submit pyspark word-count.py \
    --cluster=${CLUSTER} \
    -- gs://${BUCKET_NAME}/ gs://${BUCKET_NAME}/

# delete the cloud Dataproc cluster
gcloud dataproc clusters delete $CLUSTER

# delete the Dataproc bucket
# TO DO: find how to get bucket name
# gsutil rb [BUCKET_NAME]