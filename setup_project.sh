source ./config.sh

gcloud auth login ${USER_EMAIL}
echo "### "
echo "### Creating new project"
echo "### Add --folder=261046259366 flag if you want to add to the google.com:experimental folder"
gcloud projects create ${NEW_PROJECT_ID}

echo "### "
echo "### Assigning billing account"
echo "### "
gcloud alpha billing projects link ${NEW_PROJECT_ID} --billing-account=${BILLING_ACCOUNT_ID}
---------------------------------------
echo "### "
echo "### Giving ownership to CE group"
echo "### "
gcloud projects add-iam-policy-binding ${NEW_PROJECT_ID} --member='group:gps-ce-all@google.com' --role='roles/owner'

echo "### "
echo "### Set default project"
echo "### "
gcloud config set project ${NEW_PROJECT_ID}

echo "### "
echo "### Enable prerequisite APIs"
echo "### "
gcloud services enable drive.googleapis.com sheets.googleapis.com automl.googleapis.com

echo "### "
echo "### Create service account and download token"
echo "### "
gcloud iam service-accounts create econ-demo --description="Service account for economic development econ-demo" --display-name="econ-demo"
gcloud projects add-iam-policy-binding ${NEW_PROJECT_ID} --member="serviceAccount:econ-demo@${NEW_PROJECT_ID}.iam.gserviceaccount.com" --role="roles/bigquery.admin"
gcloud projects add-iam-policy-binding ${NEW_PROJECT_ID} --member="serviceAccount:econ-demo@${NEW_PROJECT_ID}.iam.gserviceaccount.com" --role="roles/automl.serviceAgent"
gcloud projects add-iam-policy-binding ${NEW_PROJECT_ID} --member="serviceAccount:econ-demo@${NEW_PROJECT_ID}.iam.gserviceaccount.com" --role="roles/automl.admin"
gcloud iam service-accounts add-iam-policy-binding econ-demo@${NEW_PROJECT_ID}.iam.gserviceaccount.com --member="user:${USER_EMAIL}" --role='roles/iam.serviceAccountTokenCreator'
gcloud iam service-accounts keys create token.json --iam-account=econ-demo@${NEW_PROJECT_ID}.iam.gserviceaccount.com