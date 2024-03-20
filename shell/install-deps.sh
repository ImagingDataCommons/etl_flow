#!/usr/bin/env bash

echo "export HOME=/home/circleci/${CIRCLE_PROJECT_REPONAME}" >> $BASH_ENV

# Install our primary python libraries
# If we're not on CircleCI, or we are but the lib directory isn't there (cache miss), install lib
if [ -z "${CI}" ] || [ ! -d "${HOME}/lib" ]; then
    echo "Installing Python Libraries..."
    pip3 install -r $PWD/requirements.txt -t $HOME/lib --upgrade --only-binary all
#    pip3 install -r $PWD/requirements.txt --upgrade --only-binary all
else
    echo "Using restored cache for Python Libraries"
fi

