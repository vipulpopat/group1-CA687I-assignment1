# ----- Create a MySQL instance to store the Apache Ranger policies
export CLOUD_SQL_NAME=cloudsql-mysql
gcloud sql instances create ${CLOUD_SQL_NAME} \
  --tier=db-n1-standard-1 --region=${REGION}


# ----- Set the instance password for the user root connecting from any host
gcloud sql users set-password root \
  --host=% --instance ${CLOUD_SQL_NAME} --password mysql-root-password-99


# ----- Encrypt the passwords

# ----- Create a Key Management Service (KMS) keyring to hold your keys
gcloud kms keyrings create my-keyring --location global


# ----- Create a Key Management Service (KMS) cryptographic key to encrypt your passwords
gcloud kms keys create my-key \
  --location global \
  --keyring my-keyring \
  --purpose encryption


# ----- Encrypt your Ranger admin user’s password using the key
echo "ranger-admin-password-99" | \
gcloud kms encrypt \
  --location=global \
  --keyring=my-keyring \
  --key=my-key \
  --plaintext-file=- \
  --ciphertext-file=ranger-admin-password.encrypted


# ----- Encrypt your Ranger database admin user’s password using the key
echo "ranger-db-admin-password-99" | \
gcloud kms encrypt \
  --location=global \
  --keyring=my-keyring \
  --key=my-key \
  --plaintext-file=- \
  --ciphertext-file=ranger-db-admin-password.encrypted


# ----- Encrypt your MySQL root password using the key
echo "mysql-root-password-99" | \
gcloud kms encrypt \
  --location=global \
  --keyring=my-keyring \
  --key=my-key \
  --plaintext-file=- \
  --ciphertext-file=mysql-root-password.encrypted


# ----- Create a Cloud Storage bucket to store encrypted password files
gsutil mb -l ${REGION} gs://${PROJECT_ID}-ranger


# ----- Upload the encrypted password files to the Cloud Storage bucket.
gsutil -m cp *.encrypted gs://${PROJECT_ID}-ranger



# ----- Create the cluster

# ----- Create a Cloud Storage bucket to store the Apache Solr audit logs
gsutil mb -l ${REGION} gs://${PROJECT_ID}-solr


# ----- Export all the variables required to create the cluster
export BACKEND_CLUSTER=backend-cluster

export PROJECT_ID=$(gcloud info --format='value(config.project)')
export REGION=us-central1
export ZONE=us-central1-b
export CLOUD_SQL_NAME=cloudsql-mysql

export RANGER_KMS_KEY_URI=\
projects/${PROJECT_ID}/locations/global/keyRings/my-keyring/cryptoKeys/my-key

export RANGER_ADMIN_PWD_URI=\
gs://${PROJECT_ID}-ranger/ranger-admin-password.encrypted

export RANGER_DB_ADMIN_PWD_URI=\
gs://${PROJECT_ID}-ranger/ranger-db-admin-password.encrypted

export MYSQL_ROOT_PWD_URI=\
gs://${PROJECT_ID}-ranger/mysql-root-password.encrypted


# ----- Create the backend Dataproc cluster

gcloud beta dataproc clusters create ${BACKEND_CLUSTER} \
  --optional-components=SOLR,RANGER \
  --region ${REGION} \
  --zone ${ZONE} \
  --enable-component-gateway \
  --scopes=default,sql-admin \
  --service-account=cluster-service-account@${PROJECT_ID}.iam.gserviceaccount.com \
  --properties="\
dataproc:ranger.kms.key.uri=${RANGER_KMS_KEY_URI},\
dataproc:ranger.admin.password.uri=${RANGER_ADMIN_PWD_URI},\
dataproc:ranger.db.admin.password.uri=${RANGER_DB_ADMIN_PWD_URI},\
dataproc:ranger.cloud-sql.instance.connection.name=${PROJECT_ID}:${REGION}:${CLOUD_SQL_NAME},\
dataproc:ranger.cloud-sql.root.password.uri=${MYSQL_ROOT_PWD_URI},\
dataproc:solr.gcs.path=gs://${PROJECT_ID}-solr,\
hive:hive.server2.thrift.http.port=10000,\
hive:hive.server2.thrift.http.path=cliservice,\
hive:hive.server2.transport.mode=http"



# ----- Create a sample Hive table

# ----- Create a Cloud Storage bucket to store a sample Apache Parquet file
gsutil mb -l ${REGION} gs://${PROJECT_ID}-hive


# ----- Connect using SSH into the master node
gcloud compute ssh --zone ${ZONE} ${BACKEND_CLUSTER}-m


# ----- Once in the SSH command prompt, connect to the local HiveServer2 using Apache Beeline
beeline -u "jdbc:hive2://localhost:10000/;transportMode=http;httpPath=cliservice admin admin-password"\
  --hivevar PROJECT_ID=$(gcloud info --format='value(config.project)')

# ----- Create extarnal Hive table 'crypto' from transformed csv collections

CREATE EXTERNAL TABLE IF NOT EXISTS crypto(
  currency STRING,
  trade_date DATE,
  open_time STRING, 
  close_time STRING,
  open_price DOUBLE, 
  high_price DOUBLE, 
  low_price DOUBLE, 
  close_price DOUBLE,
  asset_volume DOUBLE, 
  number_of_trades INT)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION 'gs://assignment1_resources/crypto/transformed_data/'
  TBLPROPERTIES ('external.table.purge'='true');


# ----- Verify that the table was created correctly

SELECT * FROM crypto where currency='ADA' LIMIT 10



# Exit the Beeline CLI
!quit


# Take note of the internal DNS name of the backend master
hostname -A | tr -d '[:space:]'; echo


# Exit the SSH command line
exit