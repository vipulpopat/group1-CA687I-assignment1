# -----Create a topology

# -----Clone the Dataproc initialization-actions GitHub repository
git clone https://github.com/GoogleCloudDataproc/initialization-actions.git


# -----Create a topology for the backend cluster
export KNOX_INIT_FOLDER=`pwd`/initialization-actions/knox
cd ${KNOX_INIT_FOLDER}/topologies/
mv example-hive-nonpii.xml hive-us-transactions.xml


# -----Edit the topology file
vi hive-us-transactions.xml


# Add the data analyst sample LDAP user identity “sara”
<param>
   <name>hive.acl</name>
   <value>admin,margarita;*;*</value>
</param>


# -----Change the HIVE url to point to the backend cluster Hive service
<service>
  <role>HIVE</role>
  <url>http://<backend-master-internal-dns-name>:10000/cliservice</url>
</service>



# -----Configure the SSL/TLS certificate

# -----Edit the Apache Knox general configuration file
vi ${KNOX_INIT_FOLDER}/knox-config.yaml


# -----Replace HOSTNAME by the external DNS name of  master node
certificate_hostname: localhost



# -----Spin up the proxy cluster

# -----Create a Cloud Storage bucket to provide the configurations
gsutil mb -l ${REGION} gs://${PROJECT_ID}-knox


# -----Copy all the files from the Knox initialization action folder into the bucket
gsutil -m cp -r ${KNOX_INIT_FOLDER}/* gs://${PROJECT_ID}-knox


# -----Export all the variables required to create the cluster
export PROXY_CLUSTER=proxy-cluster
export PROJECT_ID=$(gcloud info --format='value(config.project)')
export REGION=us-central1
export ZONE=us-central1-b


# -----Create the proxy cluster
gcloud dataproc clusters create ${PROXY_CLUSTER} \
  --region ${REGION} \
  --zone ${ZONE} \
  --service-account=cluster-service-account@${PROJECT_ID}.iam.gserviceaccount.com \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/knox/knox.sh \
  --metadata knox-gw-config=gs://${PROJECT_ID}-knox


# -----Verify connection through proxy

# -----Connect to its master node using SSH
gcloud compute ssh --zone ${ZONE} ${PROXY_CLUSTER}-m


# -----Run a query 
beeline -u "jdbc:hive2://localhost:8443/;\
ssl=true;sslTrustStore=/usr/lib/knox/data/security/keystores/gateway-client.jks;trustStorePassword=secret;\
transportMode=http;httpPath=gateway/hive-us-transactions/hive"\
  -e "SELECT trade_date,open_time,open_price FROM crypto where currency='ADA' LIMIT 10;"\
  -n admin -p admin-password


# -----Add user to authentication store

# -----Install the LDAP utils
sudo apt-get install ldap-utils


# -----Create an LDAP Data Interchange Format (LDIF) file
export USER_ID=margarita

printf '%s\n'\
  "# entry for user ${USER_ID}"\
  "dn: uid=${USER_ID},ou=people,dc=hadoop,dc=apache,dc=org"\
  "objectclass:top"\
  "objectclass:person"\
  "objectclass:organizationalPerson"\
  "objectclass:inetOrgPerson"\
  "cn: ${USER_ID}"\
  "sn: ${USER_ID}"\
  "uid: ${USER_ID}"\
  "userPassword:${USER_ID}-password"\
> new-user.ldif

# -----Add the user ID to the LDAP directory
ldapadd -f new-user.ldif \
  -D 'uid=admin,ou=people,dc=hadoop,dc=apache,dc=org' \
  -w 'admin-password' \
  -H ldap://localhost:33389


# Verify that the new user was added
ldapsearch -b "uid=${USER_ID},ou=people,dc=hadoop,dc=apache,dc=org" \
  -D 'uid=admin,ou=people,dc=hadoop,dc=apache,dc=org' \
  -w 'admin-password' \
  -H ldap://localhost:33389


# -----Take note of the internal DNS name of the proxy master
hostname -A | tr -d '[:space:]'; echo


# -----Exit the SSH command line
exit
