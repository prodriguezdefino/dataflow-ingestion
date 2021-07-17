#!/usr/bin/env bash

PROJECT=$1
SCHEMA_FILENAME=$2
SCHEMA_LOCATION=$3$SCHEMA_FILENAME
QPS=$4
TOPIC=$5
REGIONS=( "${@:6}" )

for REGION in "${REGIONS[@]}";
  do
    gcloud beta dataflow flex-template run pubsub-json-datagen-$REGION --project=$PROJECT --region=$REGION --template-file-gcs-location=gs://dataflow-templates/latest/flex/Streaming_Data_Generator --parameters schemaLocation=$SCHEMA_LOCATION,qps=$QPS,topic=$TOPIC,maxNumWorkers=50,enableStreamingEngine=true
  done

echo ""
echo "Launched templates"


