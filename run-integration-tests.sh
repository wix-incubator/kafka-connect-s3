#!/usr/bin/env bash

set -e

trap "docker-compose down" EXIT

if uname | grep -q Linux; then
    export DOCKER_BIND_IP=$(ip addr | grep 'eth0:' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/')
elif uname | grep -q Darwin; then
    export DOCKER_BIND_IP=$(ifconfig en0 | grep inet | grep -v inet6 | awk '{print $2}')
else
    echo "Unsupported operating system."
    exit 1
fi

echo -e "\nDocker bind IP address: $DOCKER_BIND_IP\n"

export KAFKA_HOST=$DOCKER_BIND_IP
export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${KAFKA_HOST}:9092"
export KAFKA_BROKERS="${KAFKA_HOST}:9092"

# Building connector JAR and starting test cluster
mvn clean package
docker-compose up -d
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
--data "@src/test/resources/test-connector.json" \
http://localhost:8083/connectors

# Running integration tests
mvn verify
