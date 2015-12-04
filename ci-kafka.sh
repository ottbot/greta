#!/usr/bin/env bash

set -e

SCALA_VERSION=2.11
KAFKA_VERSION=0.9.0.0
KAFKA_DIR=kafka_dist

get_abs_filename() {
    # $1 : relative filename
    echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

KAFKA_PATH=$(get_abs_filename $KAFKA_DIR)

case "$1" in
    setup)
        if [ ! -e $KAFKA_DIR ]; then
            curl http://apache.mirrorcatalogs.com/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz | tar -xz
            mv kafka_${SCALA_VERSION}-${KAFKA_VERSION} $KAFKA_DIR
        else
            echo "cache hit!"
        fi

        cd $KAFKA_DIR

        if grep -q auto.create.topics config/server.properties; then
            sed 's|auto.create.topics.enable.*|auto.create.topics.enable=false|' config/server.properties -i.bak
        else
            echo "auto.create.topics.enable=false" >> config/server.properties
        fi

        sed 's|log.dirs=.*|log.dirs='$KAFKA_PATH'/kafka-data|' config/server.properties -i.bak
        sed 's|log.dirs=.*|log.dirs='$KAFKA_PATH'/kafka-data|' config/server.properties -i.bak
        sed 's|dataDir=.*|dataDir='$KAFKA_PATH'/zk-data|' config/zookeeper.properties -i.bak
        ;;
    start-zk)
        ${KAFKA_PATH}/bin/zookeeper-server-start.sh ${KAFKA_PATH}/config/zookeeper.properties
        ;;
    start-kafka)
        ${KAFKA_PATH}/bin/kafka-server-start.sh ${KAFKA_PATH}/config/server.properties
        ;;
    start)
        ${KAFKA_PATH}/bin/zookeeper-server-start.sh ${KAFKA_PATH}/config/zookeeper.properties &
        ${KAFKA_PATH}/bin/kafka-server-start.sh ${KAFKA_PATH}/config/server.properties &
        ;;
    stop)
        ${KAFKA_PATH}/bin/kafka-server-stop.sh
        ${KAFKA_PATH}/bin/zookeeper-server-stop.sh
        ;;
    create-test-topic)
        ${KAFKA_PATH}/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic greta-tests --partitions 1 --replication-factor 1
        ;;
    publish-to-test-topic)
        echo "boot" | ${KAFKA_PATH}/bin/kafka-console-producer.sh --topic greta-tests --broker-list "localhost:9092"
        ;;

    *)
        echo "noop"
        exit 1
        ;;
esac
