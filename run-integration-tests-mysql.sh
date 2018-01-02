#!/usr/bin/env bash

set -e

trap "docker-compose down" EXIT

# Building connector JAR and starting test cluster
mvn clean package
docker-compose -f docker-compose-mysql.yml up -d
sleep 10

# Creating topic for test messages
docker-compose exec broker \
kafka-topics --zookeeper zookeeper:2181 \
--topic test-topic \
--create \
--partitions 3 \
--replication-factor 1

sleep 10

# Submitting JSON configuration
curl -H "Content-Type: application/json" \
--data "@src/test/resources/test-connector-S3-Mysql.json" \
http://localhost:8083/connectors

# Running integration tests
mvn verify
