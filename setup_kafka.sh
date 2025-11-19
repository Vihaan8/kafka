#!/bin/bash

echo "Patient Monitoring System - Kafka Setup"
echo ""

if ! docker ps > /dev/null 2>&1; then
    echo "Error: Docker is not running."
    exit 1
fi

echo "Docker is running"
echo ""

if ! docker ps | grep -q kafka; then
    echo "Error: Kafka container is not running."
    echo "Run: docker-compose up -d"
    exit 1
fi

echo "Kafka container is running"
echo ""

echo "Creating Kafka topics..."
echo ""

echo "1. Creating patient-vitals topic..."
docker exec -it kafka kafka-topics --create \
  --topic patient-vitals \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 2>/dev/null

if [ $? -eq 0 ]; then
    echo "   Topic patient-vitals created"
else
    echo "   Topic patient-vitals already exists"
fi

echo ""

echo "2. Creating patient-alerts topic..."
docker exec -it kafka kafka-topics --create \
  --topic patient-alerts \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 2>/dev/null

if [ $? -eq 0 ]; then
    echo "   Topic patient-alerts created"
else
    echo "   Topic patient-alerts already exists"
fi

echo ""
echo "Verifying topics..."
echo ""

docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "Setup complete"
echo ""
echo "Next steps:"
echo "  1. Terminal 1: python consumer.py"
echo "  2. Terminal 2: python producer.py"
echo "  3. Terminal 3: python flink_processor.py (optional)"
echo "  4. Terminal 4: python anomaly_detector.py (optional)"
echo "  5. Terminal 5: streamlit run dashboard.py"
echo ""