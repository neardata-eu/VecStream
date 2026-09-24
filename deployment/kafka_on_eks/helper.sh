#!/bin/bash

# Kafka Helper Script for Kafka on EKS

case "$1" in
  create-kafka-cli-pod)
    kubectl -n kafka run --restart=Never --image=quay.io/strimzi/kafka:0.47.0-kafka-3.9.0 kafka-cli -- /bin/sh -c "exec tail -f /dev/null"
    echo "Waiting for kafka-cli pod to be ready..."
    kubectl wait --for=condition=ready pod/kafka-cli -n kafka --timeout=60s
    ;;
  delete-kafka-cli-pod)
    kubectl -n kafka delete pod kafka-cli
    ;;
  get-kafka-pods)
    kubectl get pods -n kafka
    ;;
  get-all-kafka-namespace)
    kubectl get all -n kafka
    ;;
  describe-kafka-cluster)
    kubectl describe kafka kafka-on-eks -n kafka
    ;;
  get-kafka-nodes)
    kubectl get nodes -l karpenter.k8s.aws/instance-family=r8g -o wide
    ;;
  get-kafka-brokers)
    kubectl -n kafka get pod -l strimzi.io/pool-name=broker
    ;;
  get-kafka-controllers)
    kubectl -n kafka get pod -l strimzi.io/pool-name=controller
    ;;
  deploy-kafka-producer-consumer)
    kubectl apply -f examples/kafka-producers-consumers.yaml
    ;;
  verify-kafka-producer)
    kubectl -n kafka logs $(kubectl -n kafka get pod -l app=java-kafka-producer -o jsonpath='{.items[*].metadata.name}')
    ;;
  verify-kafka-consumer)
    kubectl -n kafka logs $(kubectl -n kafka get pod -l app=java-kafka-consumer -o jsonpath='{.items[*].metadata.name}')
    ;;
  list-topics-via-cli)
    kubectl -n kafka exec kafka-cli -- bin/kafka-topics.sh \
      --list \
      --bootstrap-server kafka-on-eks-kafka-bootstrap:9092
    ;;
  describe-topic)
    TOPIC=${2:-my-topic}
    kubectl -n kafka exec kafka-cli -- bin/kafka-topics.sh \
      --describe \
      --topic $TOPIC \
      --bootstrap-server kafka-on-eks-kafka-bootstrap:9092
    ;;
  get-strimzi-operator)
    kubectl -n strimzi-system get pods
    ;;
  debug-kafka-connectivity)
    echo "=== Kafka Connectivity Debug ==="
    echo "1. Checking Kafka brokers:"
    kubectl -n kafka get pod -l strimzi.io/pool-name=broker
    echo ""
    echo "2. Checking Kafka controllers:"
    kubectl -n kafka get pod -l strimzi.io/pool-name=controller
    echo ""
    echo "3. Checking Kafka service:"
    kubectl -n kafka get svc kafka-on-eks-kafka-bootstrap
    echo ""
    echo "4. Testing connectivity from kafka-cli pod:"
    kubectl -n kafka exec kafka-cli -- bin/kafka-broker-api-versions.sh --bootstrap-server kafka-on-eks-kafka-bootstrap:9092 2>/dev/null || echo "Failed to connect to Kafka brokers (ensure kafka-cli pod exists)"
    echo ""
    echo "5. Listing all topics:"
    kubectl -n kafka exec kafka-cli -- bin/kafka-topics.sh --list --bootstrap-server kafka-on-eks-kafka-bootstrap:9092 2>/dev/null || echo "Failed to list topics"
    ;;
  get-external-bootstrap-servers)
    echo "External Kafka Bootstrap Servers (for EC2 clients in same VPC):"
    kubectl get kafka kafka-on-eks -n kafka -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}{"\n"}'
    ;;
  *)
    echo "Kafka Helper Script - Cluster management and validation commands"
    echo ""
    echo "Usage: $0 {COMMAND}"
    echo ""
    echo "Kafka CLI Pod:"
    echo "  create-kafka-cli-pod              - Create Kafka CLI pod for testing"
    echo "  delete-kafka-cli-pod              - Delete Kafka CLI pod"
    echo ""
    echo "Kafka Resources:"
    echo "  get-kafka-pods                    - Get all Kafka pods"
    echo "  get-kafka-brokers                 - Get Kafka broker pods"
    echo "  get-kafka-controllers             - Get Kafka controller pods"
    echo "  describe-kafka-cluster            - Describe Kafka cluster resource"
    echo "  get-kafka-nodes                   - Get nodes running Kafka pods (r8g instances)"
    echo ""
    echo "Topic Management:"
    echo "  list-topics-via-cli               - List topics using Kafka CLI"
    echo "  describe-topic [topic-name]       - Describe a specific topic (default: my-topic)"
    echo ""
    echo "Producer/Consumer:"
    echo "  deploy-kafka-producer-consumer    - Deploy Kafka producers and consumers"
    echo "  verify-kafka-producer             - Check producer logs"
    echo "  verify-kafka-consumer             - Check consumer logs"
    echo ""
    echo ""
    echo "Operators:"
    echo "  get-strimzi-operator              - Get Strimzi operator pods"
    echo ""
    echo "Debugging:"
    echo "  debug-kafka-connectivity          - Debug Kafka broker connectivity and topics"
    echo "  get-external-bootstrap-servers    - Get external bootstrap servers for VPC clients"
    exit 1
esac