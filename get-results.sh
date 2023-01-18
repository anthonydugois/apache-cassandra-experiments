#!/bin/sh

cd ./archives

scp nancy.g5k:~/apache-cassandra-experiments/archives/$1 .

tar --force-local -xzf $1

