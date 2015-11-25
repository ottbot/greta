#!/usr/bin/env bash

set -e

VERSION=2.9.2-0.8.2.2

KAFKA_DIR=kafka_dist

if [ ! -e $KAFKA_DIR ]; then
    curl http://mirror.olnevhost.net/pub/apache/kafka/0.8.2.2/kafka_${VERSION}.tgz | tar -xz
    mv kafka_${VERSION} $KAFKA_DIR

    echo "auto.create.topics.enable = false" >> $KAFKA_DIR/config/server.properties
fi

cd $KAFKA_DIR

./bin/zookeeper-server-start.sh config/zookeeper.properties &
./bin/kafka-server-start.sh config/server.properties &

sleep 10

./bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic greta-tests --partitions 1 --replication-factor 1 || true
