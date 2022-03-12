# ----- Set up the project

# ----- Create a project. Replace <project-id> placeholder with your own value
export PROJECT_ID=<project-id>
gcloud projects create ${PROJECT_ID}

# ----- Set the project ID in the Cloud SDK properties
gcloud config set project ${PROJECT_ID}

# ----- List the available billing accounts
gcloud alpha billing accounts list

# ----- Enable billing for the project. Replace the <billing-account-id> placeholder by your chosen billing account id.
gcloud alpha billing projects link ${PROJECT_ID} \
  --billing-account <billing-account-id>

# ----- Enable the Cloud APIs for Dataproc, Cloud SQL, and Cloud Key Management Service (KMS)
gcloud services enable dataproc.googleapis.com sqladmin.googleapis.com \
  cloudkms.googleapis.com


# ----- In Cloud Shell, set environment variables with the ID your project and the region and zones where the Dataproc clusters will be located
export PROJECT_ID=$(gcloud info --format='value(config.project)')
export REGION=us-central1
export ZONE=us-central1-b



# ----- Set up a service account

# ----- Create a service account that will be used by the cluster to be authenticated as
gcloud iam service-accounts create cluster-service-account \
  --description="The service account for the cluster to be authenticated as." \
  --display-name="Cluster service account"

# ----- Add roles to the service account 
bash -c 'array=( dataproc.worker cloudsql.editor cloudkms.cryptoKeyDecrypter )
for i in "${array[@]}"
do
  gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member "serviceAccount:cluster-service-account@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role roles/$i
done'
