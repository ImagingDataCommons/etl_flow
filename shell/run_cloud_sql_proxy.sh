#!/bin/bash

#Script to launch the CLOUD SQL proxy. Expected to be run from cron
set -x
logger "Starting run_cloud_sql_proxy.sh"

echo PWD $(pwd)
whoami
DIRNAME=`dirname "$0"`

logger DIRNAME = $DIRNAME

source $DIRNAME/.env

logger Starting "$RUN_CLOUD_SQL_PROXY"

${RUN_CLOUD_SQL_PROXY}
logger Error=$?
logger Exiting from run_cloud_sql_proxy.sh