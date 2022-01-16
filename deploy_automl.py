from google.cloud import automl_v1beta1
from google.oauth2 import service_account
import global_variables

automl = automl_v1beta1.TablesClient(
    credentials=service_account.Credentials.from_service_account_file('token.json'),
    project=global_variables.project,region=global_variables.region)

deploy = automl.deploy_model(    
    model_display_name='labor_force_prediction',
)
print("Deploying model...")
print("Deploying: {}".format(deploy.operation))
print("Deploy model operation name: {}".format(deploy.operation.name))

def callback(operation_future):
   result = operation_future.result()

deploy.add_done_callback(callback)