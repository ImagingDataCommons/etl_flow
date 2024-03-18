mkdir ./json
mkdir ./txt

gsutil cp "gs://${ETL_FLOW_DEPLOYMENT_BUCKET}/${ETL_FLOW_ENV_FILE}" ./.env
