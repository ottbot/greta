#!/usr/bin/env bash

set -e

SCALA_VERSION=2.11
KAFKA_VERSION=0.9.0.0

KAFKA_DIR=kafka_dist

if [ ! -e $KAFKA_DIR ]; then
    curl http://apache.mirrorcatalogs.com/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz | tar -xz

    mv kafka_${SCALA_VERSION}-${KAFKA_VERSION} $KAFKA_DIR

    echo "auto.create.topics.enable = false" >> $KAFKA_DIR/config/server.properties
fi

cd $KAFKA_DIR

./bin/zookeeper-server-start.sh config/zookeeper.properties &
./bin/kafka-server-start.sh config/server.properties &

sleep 10

./bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic greta-tests --partitions 1 --replication-factor 1 || true
