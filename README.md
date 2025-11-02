# Real-Time Web Server Log Analysis

## Overview
A big data analytics system that processes Apache/NASA HTTP server logs in real-time to detect error-prone URLs, peak usage times, and anomalies.

## Architecture
- **Data Ingestion**: Kafka for real-time log streaming
- **Storage**: Hadoop HDFS for distributed storage
- **Processing**: MapReduce for batch processing + Spark Streaming for real-time analysis
- **Analytics**: Error detection, usage pattern analysis, anomaly detection

## Components
1. Log Generator (simulates web server logs)
2. Kafka Producer (streams logs to Kafka)
3. Kafka Consumer (processes logs from Kafka)
4. Hadoop Integration (HDFS storage + MapReduce)
5. Analytics Engine (real-time monitoring and alerts)

## Quick Start
```bash
# Start Kafka
cd kafka_integration && ./start-kafka.sh

# Run log generator
python log_generator/generate_logs.py

# Start analytics
python analytics/real_time_analyzer.py
```