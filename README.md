# Dataflow Ingestion Pipelines

Contains experimentations and custom implementations to demonstrate different approaches for common patterns.

To generate data for both pipeline types the data generator template can be used. To run in this command can be executed: 
``` bash
# Where REGIONS is an array of the GCP regions to be used to publish data to the defined topic
./launch_datagen_templates.sh $PROJECT $SCHEMA_FILENAME $SCHEMA_LOCATION $QPS $TOPIC "${REGIONS[@]}"
```
And then the PS to GCS pipeline can be used to store the data into a GCS location that the batch pipelines can process.