#!/bin/sh

source venv/bin/activate

python -m experiment.run "$@"
