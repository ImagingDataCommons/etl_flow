#!/usr/bin/env bash

##if [ -n "$CI" ]; then
##    export HOME=/home/circleci/${CIRCLE_PROJECT_REPONAME}
##    export HOMEROOT=/home/circleci/${CIRCLE_PROJECT_REPONAME}
##    # Clone dependencies
##    COMMON_BRANCH=master
##    if [[ ${CIRCLE_BRANCH} =~ idc-(prod|uat|test).* ]]; then
##        COMMON_BRANCH=$(awk -F- '{print $1"-"$2}' <<< ${CIRCLE_BRANCH})
##    fi
##    echo "Cloning IDC-Common branch ${COMMON_BRANCH}..."
##    git clone -b ${COMMON_BRANCH} https://github.com/ImagingDataCommons/IDC-Common.git
##else
##    export $(cat /home/vagrant/parentDir/secure_files/idc/.env | grep -v ^# | xargs) 2> /dev/null
##    export HOME=/home/vagrant
##    export HOMEROOT=/home/vagrant/API
##fi
##
### Remove .pyc files; these can sometimes stick around and if a
### model has changed names it will cause various load failures
##find . -type f -name '*.pyc' -delete
##
##export DEBIAN_FRONTEND=noninteractive
##
##apt-get update -qq
##
### Install and update apt-get info
##echo "Preparing System..."
##apt-get -y install software-properties-common
##
##if [ -n "$CI" ]; then
##    echo 'download mysql public build key'
##    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv A8D3785C
##    echo 'mysql build key update done.'
##    wget https://dev.mysql.com/get/mysql-apt-config_0.8.29-1_all.deb
##    apt-get install -y lsb-release
##    dpkg -i mysql-apt-config_0.8.29-1_all.deb
##fi
##
##apt-get update -qq
##
### Install apt-get dependencies
##echo "Installing Dependencies..."
##if [ -n "$CI" ]; then
#apt-get install -y --force-yes unzip libffi-dev libssl-dev libmysqlclient-dev python3-mysqldb python3-dev libpython3-dev git ruby g++ curl dos2unix python3.5
#apt-get install -y --force-yes mysql-client
#else
#    apt-get install -qq -y --force-yes unzip libffi-dev libssl-dev libmysqlclient-dev python3-mysqldb python3-dev libpython3-dev git ruby g++ curl dos2unix python3.5 mysql-client-5.7
#fi
#echo "Dependencies Installed"
#
## If this is local development, clean out lib for a re-structuring
#if [ -z "${CI}" ]; then
#    # Clean out lib to prevent confusion over multiple builds in local development
#    # and prep for local install
#    echo "Emptying out ${HOMEROOT}/lib/ ..."
#fi
#
## Install PIP + Dependencies
#echo "Installing pip3..."
#curl --silent https://bootstrap.pypa.io/get-pip.py | python3

# Install our primary python libraries
# If we're not on CircleCI, or we are but the lib directory isn't there (cache miss), install lib
if [ -z "${CI}" ] || [ ! -d "lib" ]; then
    echo "Installing Python Libraries..."
    pip3 install -r $PWD/requirements.txt -t $HOME/lib --upgrade --only-binary all
#    pip3 install -r $PWD/requirements.txt --upgrade --only-binary all
else
    echo "Using restored cache for Python Libraries"
fi
pip3 list

## Install Google Cloud SDK
## If we're not on CircleCI or we are but google-cloud-sdk isn't there, install it
#if [ -z "${CI}" ] || [ ! -d "/usr/lib/google-cloud-sdk" ]; then
#    echo "Installing Google Cloud SDK..."
#    export CLOUDSDK_CORE_DISABLE_PROMPTS=1
#    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
#    apt-get -y install apt-transport-https ca-certificates
#    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
#    apt-get update -qq
#    apt-get -y install google-cloud-sdk
#    apt-get -y install google-cloud-sdk-app-engine-python
#    echo "Google Cloud SDK Installed"
#fi

