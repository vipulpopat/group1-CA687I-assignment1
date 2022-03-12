# -----Create a firewall rule

# -----Create a firewall rule that opens TCP port 8443
gcloud compute firewall-rules create allow-knox\
  --project=${PROJECT_ID} --direction=INGRESS --priority=1000 \
  --network=default --action=ALLOW --rules=tcp:8443 \
  --target-tags=knox-gateway \
  --source-ranges=<your-public-ip>/32

# -----Apply the network tag from the firewall rule to the proxy cluster master node
gcloud compute instances add-tags ${PROXY_CLUSTER}-m --zone=${ZONE} \
  --tags=knox-gateway


# -----Create an SSH tunnel
# -----Generate the command to create the tunnel
echo "gcloud compute ssh ${PROXY_CLUSTER}-m \
  --project ${PROJECT_ID} \
  --zone ${ZONE} \
  -- -L 8443:localhost:8443"

# -----Query Hive data

# -----Enter the following query
SELECT 	`trade_date`,
		`open_time`,
		`open_price`
FROM `crypto`


