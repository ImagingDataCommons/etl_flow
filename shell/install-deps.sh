#!/usr/bin/env bash

# Install our primary python libraries
# If we're not on CircleCI, or we are but the PWD lib directory isn't there (cache miss), install lib
if [ -z "${CI}" ] || [ ! -d "lib" ]; then
    echo "Installing Python Libraries..."
    pip3 install -r $PWD/requirements.txt -t lib --upgrade --only-binary all
#    pip3 install -r $PWD/requirements.txt --upgrade --only-binary all
else
    echo "Using restored cache for Python Libraries"
fi

