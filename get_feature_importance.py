from google.cloud import bigquery
import global_variables
import pandas as pd
import numpy as np
import gspread
import df2gspread as d2g

# Construct a BigQuery client object.
bq = bigquery.Client(project=global_variables.project)

# Find all the BQ datasets
datasets = list(bq.list_datasets()) 

# Find dataset of batch prediction results
for dataset in datasets:
    if dataset.dataset_id.startswith('prediction_'): # Find batch prediction
        pred_dataset = dataset.dataset_id
        print("Batch prediction dataset found: " + pred_dataset)

state_csv = pd.read_csv('state_fips.csv',dtype=str)
states = state_csv['postal_code'].values 

## Save results as BigQuery tables and dataframe for each state ##
for i in range(len(states)):
    query = """SELECT geo_id as tract, feature_importance.* FROM `{0}.predictions` 
    WHERE state = "{1}" 
    """
    updated_query = query.format(pred_dataset,states[i])
    table_id = global_variables.project + "." + pred_dataset + ".feature_importance_" + states[i]
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = 'WRITE_TRUNCATE'
    query_job = bq.query(updated_query,job_config=job_config)
    query_job.result()  # Waits for job to complete.
    print("Query results loaded to the table {}".format(table_id))
    dataframe = (
        bq.query(updated_query,job_config=job_config)
        .result()
        .to_dataframe(
            create_bqstorage_client=False
        )
    )
    print(dataframe.head())

# Export table to GCS
'''
bucket_name = 'automl-williszhang'
destination_uri_1 = "gs://{}/{}".format(bucket_name, "feature_importance_1.csv")
dataset_ref = bigquery.DatasetReference(project, pred_dataset_1)
table_ref = dataset_ref.table('feature_importance_1')

extract_job_1 = bq.extract_table(
    table_ref,
    destination_uri_1,
    # Location must match that of the source table.
    location="US",
)  # API request
extract_job_1.result()  # Waits for job to complete.

print(
    "Exported {} to {}".format(table_ref, destination_uri_1)
)
'''