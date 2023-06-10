#!/bin/bash

KAFKA_VERSION=3.4.0
SCALA_VERSION=2.13

DOWNLOAD_URL="https://dlcdn.apache.org/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz"

wget "$DOWNLOAD_URL" -O "/tmp/kafka_out.tgz"

tar xfz /tmp/kafka_out.tgz -C /opt

rm /tmp/kafka_out.tgz

ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka
