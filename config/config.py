# Configuration settings for the log analysis system

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic_name': 'web_server_logs',
    'auto_offset_reset': 'latest',
    'enable_auto_commit': True,
    'group_id': 'log_analyzer_group'
}

# Hadoop Configuration
HADOOP_CONFIG = {
    'namenode_host': 'localhost',
    'namenode_port': 9000,
    'hdfs_path': '/user/logs/web_server/',
    'replication_factor': 1
}

# Analytics Configuration
ANALYTICS_CONFIG = {
    'error_threshold': 10,  # Number of errors per minute to trigger alert
    'anomaly_threshold': 100,  # Requests per minute from single IP
    'window_size_minutes': 5,  # Time window for analysis
    'top_urls_count': 10  # Number of top URLs to track
}

# Log Format Configuration
LOG_FORMAT = {
    'apache_common': r'(\S+) \S+ \S+ \[([\w:/\s+\-]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|-)',
    'fields': ['ip', 'timestamp', 'method', 'url', 'protocol', 'status_code', 'response_size']
}