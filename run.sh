#!/bin/sh

# set local environment variables
PROJECT='book-recommendations-259715 '
BUCKET_NAME='bookreview_bucket'
CLUSTER='democluster'
ZONE='us-west1-a'

# cluster configuration
NUM_MASTERS=1
MASTER_MACHINE_TYPE='n1-standard-2'
NUM_WORKERS=3
WORKER_MACHINE_TYPE='n1-standard-2'

# TO DO: write configuration yaml

# create a cloud Dataproc cluster
gcloud dataproc clusters create $CLUSTER \
    --project=${PROJECT} \
    --zone=${ZONE} \
    --image-version 1.3 \
    --num-masters=${NUM_MASTERS} \
    --master-machine-type=${MASTER_MACHINE_TYPE} \
    --num-workers=${NUM_WORKERS} \
    --worker-machine-type=${WORKER_MACHINE_TYPE} \
    --metadata 'MINICONDA_VARIANT=3' \
    --metadata 'MINICONDA_VERSION=latest' \
    --initialization-actions \
    gs://dataproc-initialization-actions/conda/bootstrap-conda.sh,gs://dataproc-initialization-actions/conda/install-conda-env.sh

# zip python packages
zip -r pypackage.zip scripts/ utils/ models/

# submit job to Cloud Dataproc cluster
gcloud dataproc jobs submit pyspark test.py \
    --cluster=${CLUSTER} \
    --py-files pypackage.zip \
    -- gs://${BUCKET_NAME}/

# delete the cloud Dataproc cluster
gcloud dataproc clusters delete $CLUSTER

# delete the Dataproc bucket
# TO DO: find how to get bucket name
# gsutil rb [BUCKET_NAME]