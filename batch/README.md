# Batch Pipelines 

Contains examples on how to ingest data from Parquet based files in GCS into BigQuery (using Avro as the staging format) and also how to store that data into a different format into GCS. 

## Parquet to CSV

Contains two implementations, one based on FileIO+Sink which creates CSV files using the first-level columns existing on the Avro schema, and another one that creates CSV "plus" files which contain a custom header, index and footer sections in addition to the common CSV format as part of the data section. This custom CSV format is implemented using a FileBasedSink implementation that creates 3 different files (one for each header, index and data sections) in order to pasivate the data of the index and header without having to construct those structures in memory while the data is being processed and written on the destination format (this is helpful for large volumes of data). 

The performance of the custom FileBasedSink implementation is comparable to the one using the simple Sink, it creates a bigger file in the destination since it this custom format contains more than just the data included on the CSV (header, indexes and footer). 

To launch the pipeline running the next maven command should be suficient: 
``` bash 
mvn compile exec:java   -Dexec.mainClass=com.example.dataflow.GCSParquetToCSV   -Dexec.cleanupDaemonThreads=false   -Dexec.args=" \
  --project=$PROJECT \
  --avroSchemaFileLocation='avro-schema.json' \
  --stagingLocation=gs://$STAGING_BUCKET/dataflow/staging \
  --tempLocation=gs://$STAGING_BUCKET/dataflow/temp \
  --numWorkers=100 \
  --maxNumWorkers=100 \
  --runner=DataflowRunner \
  --workerMachineType=n1-standard-8 \
  --usePublicIps=false \
  --region=us-central1 \
  --numShards=70 \
  --gcsPerformanceMetrics=true \
  --inputLocation=gs://$BUCKET/parquet/2021/10/08/06/*.parquet \
  --outputLocation=gs://$STAGING_BUCKET/csv/ \
  --jobName='gcs-parquet-to-csv' "

# The option --useFileIO can be added to use the FileIO+Sink implementation to compare performance
```

## Parquet to BQ

Contains a simple pipeline that demonstrates how to use Avro as the staging format for BQ FILE_LOADS ingestion. 

To launch the pipeline running the next maven command should be suficient: 
``` bash 
mvn compile exec:java   -Dexec.mainClass=com.example.dataflow.GCSParquetToBigQuery   -Dexec.cleanupDaemonThreads=false   -Dexec.args=" \
  --project=$PROJECT \
  --avroSchemaFileLocation='avro-schema.json' \
  --stagingLocation=gs://$STAGING_BUCKET/dataflow/staging \
  --tempLocation=gs://$STAGING_BUCKET/dataflow/temp \
  --numWorkers=99 \
  --maxNumWorkers=99 \
  --runner=DataflowRunner \
  --workerMachineType=n1-highmem-2 \
  --usePublicIps=false \
  --region=us-central1 \
  --gcsPerformanceMetrics=true \
  --inputLocation=gs://$BUCKET/parquet/2021/10/08/20/**.parquet \
  --outputTableSpec=$BQ_TABLE \
  --jobName='gcs-parquet-to-bq' "
```
