#!/usr/bin/env bash

PIP3=$(which pip3)
VIRTUALENV=$(which virtualenv)

if [[ -z $PIP3 ]]; then
    echo "pip3 is not available - please install python3"
    exit 1
fi

if [[ -z $VIRTUALENV ]]; then
    pip3 install virtualenv
fi

virtualenv --python=python3 venv

source venv/bin/activate

if [[ -f requirements.txt ]]; then
    pip install -r requirements.txt
fi
