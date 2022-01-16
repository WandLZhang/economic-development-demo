from google.cloud import automl_v1beta1
from google.oauth2 import service_account
import global_variables

bq_automl_baseline_uri='bq://' + global_variables.project + '.' + global_variables.staging_dataset_name + '.ml_input_automl_baseline'

client = automl_v1beta1.TablesClient(
    credentials=service_account.Credentials.from_service_account_file('token.json'),
    project=global_variables.project, region='us-central1')

batch = client.batch_predict(
    model_display_name='labor_force_prediction',
    bigquery_input_uri=bq_automl_baseline_uri,
    bigquery_output_uri='bq://' + global_variables.project,
    params={'feature_importance': 'true'}
    )

print("Batch predicting...")
print("Batch predicting: {}".format(batch.operation))
print("Prediction operation name: {}".format(batch.operation.name))

print("Batch prediction completed. {}".format(batch.result())) # blocks on result