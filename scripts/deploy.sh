#!/bin/sh

# set local environment variables
PROJECT='book-recommendations-259715 '
BUCKET_NAME='bookreview_bucket'
CLUSTER='democluster'
REGION='us-west1'
ZONE='us-west1-a'

# cluster configuration
NUM_MASTERS=1
MASTER_MACHINE_TYPE='n1-standard-4'
NUM_MASTER_LOCAL_SSDS=1
NUM_WORKERS=5
WORKER_MACHINE_TYPE='n1-standard-4'
NUM_WORKER_LOCAL_SSDS=1

gcloud config set project ${PROJECT}

cd pyspark-recommendation-engine

# submit job to Cloud Dataproc cluster
gcloud dataproc jobs submit pyspark example.py \
    --cluster=${CLUSTER} --region=${REGION} \
    --py-files pybundle.zip \
    -- gs://${BUCKET_NAME}

# delete the cloud Dataproc cluster
gcloud dataproc clusters delete $CLUSTER --region=${REGION}

# (optional) delete the Dataproc bucket
# gsutil rb gs://${BUCKET_NAME}/dataproc*