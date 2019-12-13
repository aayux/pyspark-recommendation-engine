#!/bin/sh

# set local environment variables
PROJECT='book-recommendations-259715 '
BUCKET_NAME='bookreview_bucket'
CLUSTER='democluster'
ZONE='us-west1-a'

# cluster configuration
NUM_MASTERS=1
MASTER_MACHINE_TYPE='n1-standard-4'
NUM_MASTER_LOCAL_SSDS=1
NUM_WORKERS=5
WORKER_MACHINE_TYPE='n1-standard-4'
NUM_WORKER_LOCAL_SSDS=1

gcloud config set project ${PROJECT}

# TO DO: write configuration yaml

# delete any pre-existing clusters
yes | gcloud dataproc clusters delete $CLUSTER

# create a cloud Dataproc cluster
gcloud dataproc clusters create $CLUSTER \
    --project=${PROJECT} --zone=${ZONE} --image-version 1.3 \
    --num-masters=${NUM_MASTERS} \
    --master-machine-type=${MASTER_MACHINE_TYPE} \
    --num-master-local-ssds=${NUM_MASTER_LOCAL_SSDS} \
    --num-workers=${NUM_WORKERS} \
    --worker-machine-type=${WORKER_MACHINE_TYPE} \
    --num-worker-local-ssds=${NUM_WORKER_LOCAL_SSDS} \
    --metadata 'MINICONDA_VARIANT=3' \
    --metadata 'MINICONDA_VERSION=latest' \
    --metadata 'PIP_PACKAGES=numpy==1.16.4 pandas==0.24.1 scipy==1.3.0 tensorflow' \
    --initialization-actions \
    gs://dataproc-initialization-actions/conda/bootstrap-conda.sh,gs://dataproc-initialization-actions/conda/install-conda-env.sh

gcloud dataproc clusters create $CLUSTER \
    --project=${PROJECT} --zone=${ZONE} --image-version 1.3 \
    --single-node \
    --metadata 'MINICONDA_VARIANT=3' \
    --metadata 'MINICONDA_VERSION=latest' \
    --metadata 'PIP_PACKAGES=numpy==1.16.4 pandas==0.24.1 scipy==1.3.0 tensorflow' \
    --initialization-actions \
    gs://dataproc-initialization-actions/conda/bootstrap-conda.sh,gs://dataproc-initialization-actions/conda/install-conda-env.sh

# zip python packages
zip -r pybundle.zip scripts/ utils/ models/

# submit job to Cloud Dataproc cluster
gcloud dataproc jobs submit pyspark main.py \
    --cluster=${CLUSTER} \
    --py-files pybundle.zip \
    -- gs://${BUCKET_NAME}

# delete the cloud Dataproc cluster
gcloud dataproc clusters delete $CLUSTER

# (optional) delete the Dataproc bucket
# gsutil rb gs://${BUCKET_NAME}/dataproc*