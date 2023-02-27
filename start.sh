#!/bin/sh

ROOT=$HOME/apache-cassandra-experiments

cd $ROOT

$ROOT/venv/bin/python -m experiment.run "$@"
