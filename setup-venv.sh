#!/usr/bin/env bash

if ! which python3 > /dev/null; then
    echo "python3 is not available - please install"
    exit 1
fi

python3 -m venv venv

source venv/bin/activate

if [[ -f requirements.txt ]]; then
    pip3 install --upgrade pip wheel
    pip3 install --requirement requirements.txt
fi
