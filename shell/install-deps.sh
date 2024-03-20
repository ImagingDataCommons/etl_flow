#!/usr/bin/env bash

# If python libraries haven't been installed, install them
if [ -z $(pip list| grep pydicom) ]; then
    echo "Installing Python Libraries..."
    pip3 install -r $PWD/requirements.txt --upgrade --only-binary all
else
    echo "Using restored cache for Python Libraries"
fi

