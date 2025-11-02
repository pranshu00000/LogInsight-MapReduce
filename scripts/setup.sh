#!/bin/bash

# Setup script for Real-Time Web Server Log Analysis project

echo "Setting up Real-Time Web Server Log Analysis project..."

# Create necessary directories
mkdir -p data/logs
mkdir -p data/processed
mkdir -p logs
mkdir -p temp

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Download and setup Kafka (if not already installed)
if [ ! -d "kafka_2.13-2.8.0" ]; then
    echo "Downloading Kafka..."
    wget https://downloads.apache.org/kafka/2.8.0/kafka_2.13-2.8.0.tgz
    tar -xzf kafka_2.13-2.8.0.tgz
    rm kafka_2.13-2.8.0.tgz
fi

# Create Kafka startup script
cat > start-kafka.sh << 'EOF'
#!/bin/bash

# Start Zookeeper
echo "Starting Zookeeper..."
./kafka_2.13-2.8.0/bin/zookeeper-server-start.sh ./kafka_2.13-2.8.0/config/zookeeper.properties &

# Wait for Zookeeper to start
sleep 5

# Start Kafka
echo "Starting Kafka..."
./kafka_2.13-2.8.0/bin/kafka-server-start.sh ./kafka_2.13-2.8.0/config/server.properties &

# Wait for Kafka to start
sleep 10

# Create topic
echo "Creating Kafka topic..."
./kafka_2.13-2.8.0/bin/kafka-topics.sh --create --topic web_server_logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo "Kafka setup complete!"
EOF

chmod +x start-kafka.sh

# Create sample data generator script
cat > generate-sample-data.py << 'EOF'
#!/usr/bin/env python3
import json
import sys
import os
sys.path.append('.')
from log_generator.generate_logs import LogGenerator

# Generate sample log data for testing
generator = LogGenerator()
sample_logs = []

print("Generating sample log data...")
for i in range(1000):
    log_entry = generator.generate_log_entry()
    # Parse and convert to JSON for MapReduce testing
    import re
    from config.config import LOG_FORMAT
    
    pattern = LOG_FORMAT['apache_common']
    match = re.match(pattern, log_entry)
    
    if match:
        log_data = {
            'ip': match.group(1),
            'timestamp': match.group(2),
            'method': match.group(3),
            'url': match.group(4),
            'protocol': match.group(5),
            'status_code': int(match.group(6)),
            'response_size': match.group(7),
            'raw_log': log_entry
        }
        sample_logs.append(log_data)

# Save to file
with open('data/sample_logs.json', 'w') as f:
    for log in sample_logs:
        f.write(json.dumps(log) + '\n')

print(f"Generated {len(sample_logs)} sample log entries in data/sample_logs.json")
EOF

chmod +x generate-sample-data.py

echo "Setup complete!"
echo ""
echo "Next steps:"
echo "1. Start Kafka: ./start-kafka.sh"
echo "2. Generate sample data: python generate-sample-data.py"
echo "3. Start log producer: python kafka/kafka_producer.py"
echo "4. Start analytics engine: python analytics/real_time_analyzer.py"
echo "5. Run MapReduce analysis: python hadoop/mapreduce_jobs.py data/sample_logs.json"