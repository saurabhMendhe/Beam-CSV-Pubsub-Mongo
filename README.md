# Beam-CSV-Pubsub-Mongo (GCP)
This is project is created to understand how we can leverage apache beam streaming framework to consume the raw event inform of 
CSV using data ingestion queue Pub/Sub and loading it into MongoDb
I have created the apache beam pipeline using JAVA + Gradle as dependency management and execution engine is GCP dataflow.
Also written the custom parser for CSV file using Java Iterator. Reference taken : Apache libraries.

## Arguments description:
| ARGUMENT | DESCRIPTION |
|----------|-------------|
|runner | Need to specify runner for pipeline for gcp it will be data-flow and for local it will be direct runner. |
|project | Google Cloud Platform project name. |
|stagingLocation| Google bucket where staging artifact will be uploaded. |
|filesToStage| Google bucket where staging artifact will be uploaded. |
|region| Region used for Dataflow jobs. |
|tempLocation| Google bucket for temp directory used by data flow. |
|gcpTempLocation|  |
|autoscalingAlgorithm| With autoscaling enabled, the Dataflow service automatically chooses the appropriate number of worker instances required to run your job. We use value as "THROUGHPUT_BASED". |
|maxNumWorkers| The maximum number of Compute Engine instances to be made available to your pipeline during execution. |
|topicSubscription| Susbcripction name where the data will be arrived. |

## DEVLOPMENT ENVIRONMENT
##### *Program Arguments for Data-pipeline in IDE configuration*
```
--runner=DirectRunner
--project=[project_id]
--topicSubscription=projects/[project_id]/subscriptions/[subsription_name]
--tempLocation=gs://[bucket_name]/temp
--gcpTempLocation=gs://[bucket_name]/temp
--stagingLocation=gs://[bucket_name]/stage
--filesToStage=gs://[bucket_name]/stage
--jobName=customer-data-extraction
--region=europe-west1
--autoscalingAlgorithm=THROUGHPUT_BASED
--maxNumWorkers=1
```

##### *Deploying Dataflow job via Dataflow Template and GCloud*

###### *Creates Dataflow Template*
```
gradle run -Pargs="--runner=DataflowRunner --project=[project_id] --topicSubscription=projects/[project_id]/subscriptions/[subsription_name] --jobName=customer-data-extraction --region=europe-west1 --autoscalingAlgorithm=THROUGHPUT_BASED --maxNumWorkers=1 --templateLocation=gs://[bucket_name]/template/[template_name] --tempLocation=gs://[bucket_name]/temp --stagingLocation=gs://[bucket_name]/stage"
```
###### *Deployment of Dataflow job via datflow-template through GCloud command*
```
gcloud dataflow jobs run customer-data-extraction --gcs-location=gs://[bucket_name]/template/[template_name] --region=europe-west1 --parameters  project=[project_name]
```

