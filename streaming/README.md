# Dataflow Pipeline: Pubsub to GCS (Parquet format)

This pipeline ingest JSON data from Pubsub and writes it into GCS. It includes configuration knobs to turn on/off creation of SUCCESS file per window (containing data or not) and file composition (in order to avoid small files being generated for downstream consumers).

To launch the pipeline running the next maven command should be suficient: 
``` bash 
mvn compile exec:java   -Dexec.mainClass=com.example.dataflow.PubsubToGCSParquet   -Dexec.cleanupDaemonThreads=false   -Dexec.args=" \
  --project=$PROJECT \
  --stagingLocation=gs://$SOME_BUCKET/dataflow/staging \
  --tempLocation=gs://$SOME_BUCKET/dataflow/temp \
  --inputSubscription=projects/$PROJECT/subscriptions/$SUBSCRIPTION_NAME \
  --outputDirectory=gs://$SOME_BUCKET/parquet/ \
  --tempDirectory=gs://$SOME_BUCKET/files-temp-dir/ \
  --enableStreamingEngine \
  --region=us-central1 \
  --numWorkers=1 \
  --maxNumWorkers=10 \
  --runner=DataflowRunner 
  --workerMachineType=n1-highmem-2 \
  --windowDuration=5m \
  --numShards=1000 \
  --usePublicIps=false \
  --composeSmallFiles=true \
  --fileCountPerCompose=20 \
  --composeTempDirectory=gs://$SOME_BUCKET/files-temp-dir/pre-compose \
  --jobName='pubsubtogcsparquet'"
```

To launch the data generator template run a similar script like: 
``` bash
# Where REGIONS is an array of the GCP regions to be used to publish data to the defined topic
./launch_datagen_templates.sh $PROJECT $SCHEMA_FILENAME $SCHEMA_LOCATION $QPS $TOPIC "${REGIONS[@]}"
```