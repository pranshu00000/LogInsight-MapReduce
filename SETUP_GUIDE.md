# Real-Time Web Server Log Analysis - Setup Guide

## Project Overview

This project implements a complete big data analytics pipeline for real-time web server log analysis. It demonstrates how to:

- Stream Apache/NASA HTTP server logs into Kafka
- Process logs using Hadoop MapReduce for batch analysis
- Detect error-prone URLs, peak usage times, and anomalies in real-time
- Store massive amounts of log data in HDFS

## Architecture Components

### 1. Data Generation
- **Log Generator**: Simulates realistic Apache web server logs
- **Formats**: Apache Common Log Format with realistic IP addresses, URLs, and status codes

### 2. Real-Time Streaming
- **Kafka Producer**: Streams logs to Kafka topics in real-time
- **Kafka Consumer**: Processes logs from Kafka for real-time analysis

### 3. Storage Layer
- **Hadoop HDFS**: Distributed storage for massive log datasets
- **Partitioning**: Logs organized by date for efficient querying

### 4. Processing Layer
- **MapReduce Jobs**: Batch processing for historical analysis
- **Real-Time Analytics**: Stream processing for immediate insights

### 5. Analytics & Monitoring
- **Error Detection**: Identifies error-prone URLs and patterns
- **Anomaly Detection**: Detects suspicious IP behavior
- **Usage Analytics**: Peak time analysis and traffic patterns

## Quick Start (Docker Method - Recommended)

### Prerequisites
- Docker and Docker Compose installed
- Python 3.8+ installed
- At least 4GB RAM available

### 1. Start Infrastructure
```bash
# Start Kafka and Hadoop cluster
docker-compose up -d

# Wait for services to be ready (about 2-3 minutes)
docker-compose logs -f kafka
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the Demo
```bash
# Interactive demo
python scripts/run_demo.py

# Or automated demo
python scripts/run_demo.py --auto
```

## Manual Setup (Advanced)

### 1. Install Kafka
```bash
# Download Kafka
wget https://downloads.apache.org/kafka/2.8.0/kafka_2.13-2.8.0.tgz
tar -xzf kafka_2.13-2.8.0.tgz

# Start Zookeeper
./kafka_2.13-2.8.0/bin/zookeeper-server-start.sh ./kafka_2.13-2.8.0/config/zookeeper.properties

# Start Kafka (in another terminal)
./kafka_2.13-2.8.0/bin/kafka-server-start.sh ./kafka_2.13-2.8.0/config/server.properties

# Create topic
./kafka_2.13-2.8.0/bin/kafka-topics.sh --create --topic web_server_logs --bootstrap-server localhost:9092
```

### 2. Install Hadoop
```bash
# Download Hadoop
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
tar -xzf hadoop-3.3.4.tar.gz

# Set JAVA_HOME and HADOOP_HOME
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
export HADOOP_HOME=/path/to/hadoop-3.3.4
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Format namenode
hdfs namenode -format

# Start Hadoop
start-dfs.sh
start-yarn.sh
```

## Running Individual Components

### 1. Generate Sample Data
```bash
python log_generator/generate_logs.py
```

### 2. Start Kafka Producer
```bash
python kafka_integration/kafka_producer.py
```

### 3. Start Real-Time Analytics
```bash
python analytics/real_time_analyzer.py
```

### 4. Run MapReduce Analysis
```bash
# Generate sample data first
python scripts/run_demo.py

# Run batch analysis
python hadoop/mapreduce_jobs.py data/demo_logs.json
```

## Understanding the Output

### Real-Time Dashboard
The analytics engine shows:
- **Error Alerts**: URLs with high error rates
- **Anomaly Alerts**: IPs with suspicious request patterns
- **Top URLs**: Most popular pages
- **Error-Prone URLs**: Pages with most errors

### MapReduce Results
Batch analysis provides:
- **Error Analysis**: Which URLs are failing most
- **Peak Usage**: Busiest hours of the day
- **IP Anomalies**: Potential security threats
- **URL Popularity**: Most accessed resources

## Configuration

### Kafka Settings (`config/config.py`)
```python
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic_name': 'web_server_logs',
    'group_id': 'log_analyzer_group'
}
```

### Analytics Thresholds
```python
ANALYTICS_CONFIG = {
    'error_threshold': 10,      # Errors per minute to alert
    'anomaly_threshold': 100,   # Requests per minute from single IP
    'window_size_minutes': 5    # Analysis window size
}
```

## Scaling for Production

### 1. Kafka Scaling
- Increase partitions for higher throughput
- Add more Kafka brokers for redundancy
- Tune batch sizes and compression

### 2. Hadoop Scaling
- Add more DataNodes for storage
- Increase replication factor for reliability
- Optimize MapReduce job parameters

### 3. Real-Time Processing
- Use Apache Spark Streaming for complex analytics
- Implement proper windowing and state management
- Add persistent storage for analytics state

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   ```bash
   # Check if Kafka is running
   docker-compose ps
   # Or for manual setup
   jps | grep Kafka
   ```

2. **HDFS Permission Denied**
   ```bash
   # Set proper permissions
   hdfs dfs -chmod 777 /user
   ```

3. **Python Import Errors**
   ```bash
   # Install missing dependencies
   pip install -r requirements.txt
   ```

### Monitoring

- **Kafka**: http://localhost:9092 (if using Kafka Manager)
- **Hadoop NameNode**: http://localhost:9870
- **Hadoop ResourceManager**: http://localhost:8088

## Sample Output

### Real-Time Analytics Dashboard
```
🚀 REAL-TIME WEB SERVER LOG ANALYTICS DASHBOARD
===============================================================================
⏰ Current Time: 2024-01-15 14:30:25
📊 Analysis Window: 5 minutes
📝 Logs Processed: 1,247

🚨 ERROR ALERTS:
   ⚠️  /broken-page: 15 errors in 5 min
   ⚠️  /server-error: 8 errors in 5 min

🔍 ANOMALY ALERTS:
   🚨 IP 192.168.1.100: 150 requests in 5 min

📈 TOP URLS:
   /: 245 hits
   /products: 189 hits
   /home: 156 hits
```

### MapReduce Analysis Results
```
=== ERROR ANALYSIS RESULTS ===
URL: /broken-page, Status: 404, Count: 45
URL: /server-error, Status: 500, Count: 23
URL: /missing-resource, Status: 404, Count: 18

=== PEAK USAGE ANALYSIS RESULTS ===
Hour: 09:00, Requests: 1,234
Hour: 14:00, Requests: 1,189
Hour: 16:00, Requests: 1,156

=== IP ANOMALY ANALYSIS RESULTS ===
🚨 ANOMALY - IP: 192.168.1.100, Requests: 234
🚨 ANOMALY - IP: 10.0.0.50, Requests: 156
```

## Next Steps

1. **Enhanced Analytics**: Add machine learning for better anomaly detection
2. **Visualization**: Integrate with Grafana or Kibana for dashboards
3. **Alerting**: Add email/Slack notifications for critical alerts
4. **Security**: Implement authentication and encryption
5. **Performance**: Optimize for higher throughput and lower latency

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review Docker logs: `docker-compose logs [service_name]`
3. Verify all services are running: `docker-compose ps`