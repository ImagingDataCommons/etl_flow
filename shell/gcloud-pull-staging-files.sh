mkdir ./json
mkdir ./txt

gcloud storage cp "gs://${ETL_FLOW_DEPLOYMENT_BUCKET}/${ETL_FLOW_ENV_FILE}" ./.env
