#!/usr/bin/env python3
"""
Real-Time Analytics Engine
Processes logs from Kafka in real-time and detects anomalies, errors, and patterns
"""

import json
import time
import sys
import os
from collections import defaultdict, deque
from datetime import datetime, timedelta
from threading import Thread, Lock
import signal

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_CONFIG, ANALYTICS_CONFIG
from kafka_integration.kafka_consumer import LogKafkaConsumer

class RealTimeAnalyzer:
    def __init__(self):
        self.window_size = ANALYTICS_CONFIG['window_size_minutes']
        self.error_threshold = ANALYTICS_CONFIG['error_threshold']
        self.anomaly_threshold = ANALYTICS_CONFIG['anomaly_threshold']
        
        # Thread-safe data structures
        self.lock = Lock()
        self.log_buffer = deque(maxlen=10000)  # Keep last 10k logs in memory
        
        # Analytics data
        self.error_counts = defaultdict(int)
        self.ip_request_counts = defaultdict(int)
        self.url_hit_counts = defaultdict(int)
        self.hourly_traffic = defaultdict(int)
        
        # Time-windowed data
        self.windowed_errors = deque()
        self.windowed_requests = deque()
        
        self.running = True
        
    def process_log(self, log_data):
        """Process a single log entry"""
        with self.lock:
            timestamp = datetime.now()
            
            # Add to buffer
            log_entry = {
                'timestamp': timestamp,
                'data': log_data
            }
            self.log_buffer.append(log_entry)
            
            # Update counters
            ip = log_data.get('ip', '')
            url = log_data.get('url', '')
            status_code = log_data.get('status_code', 0)
            
            # Track IP requests
            self.ip_request_counts[ip] += 1
            
            # Track URL hits
            if status_code == 200:
                self.url_hit_counts[url] += 1
            
            # Track errors
            if status_code >= 400:
                self.error_counts[url] += 1
                self.windowed_errors.append((timestamp, url, status_code))
            
            # Track requests for windowing
            self.windowed_requests.append((timestamp, ip))
            
            # Track hourly traffic
            hour_key = timestamp.strftime('%H')
            self.hourly_traffic[hour_key] += 1
    
    def cleanup_old_data(self):
        """Remove data outside the analysis window"""
        cutoff_time = datetime.now() - timedelta(minutes=self.window_size)
        
        with self.lock:
            # Clean windowed errors
            while self.windowed_errors and self.windowed_errors[0][0] < cutoff_time:
                self.windowed_errors.popleft()
            
            # Clean windowed requests
            while self.windowed_requests and self.windowed_requests[0][0] < cutoff_time:
                self.windowed_requests.popleft()
    
    def detect_error_spikes(self):
        """Detect error spikes in the current window"""
        cutoff_time = datetime.now() - timedelta(minutes=self.window_size)
        
        recent_errors = defaultdict(int)
        for timestamp, url, status_code in self.windowed_errors:
            if timestamp >= cutoff_time:
                recent_errors[url] += 1
        
        alerts = []
        for url, count in recent_errors.items():
            if count >= self.error_threshold:
                alerts.append({
                    'type': 'error_spike',
                    'url': url,
                    'error_count': count,
                    'window_minutes': self.window_size,
                    'timestamp': datetime.now().isoformat()
                })
        
        return alerts
    
    def detect_ip_anomalies(self):
        """Detect IP addresses with anomalous request patterns"""
        cutoff_time = datetime.now() - timedelta(minutes=self.window_size)
        
        recent_ip_counts = defaultdict(int)
        for timestamp, ip in self.windowed_requests:
            if timestamp >= cutoff_time:
                recent_ip_counts[ip] += 1
        
        alerts = []
        for ip, count in recent_ip_counts.items():
            if count >= self.anomaly_threshold:
                alerts.append({
                    'type': 'ip_anomaly',
                    'ip': ip,
                    'request_count': count,
                    'window_minutes': self.window_size,
                    'timestamp': datetime.now().isoformat()
                })
        
        return alerts
    
    def generate_analytics_report(self):
        """Generate current analytics report"""
        with self.lock:
            report = {
                'timestamp': datetime.now().isoformat(),
                'window_size_minutes': self.window_size,
                'total_logs_processed': len(self.log_buffer),
                'top_urls': dict(sorted(self.url_hit_counts.items(), 
                                      key=lambda x: x[1], reverse=True)[:10]),
                'error_urls': dict(sorted(self.error_counts.items(), 
                                        key=lambda x: x[1], reverse=True)[:10]),
                'top_ips': dict(sorted(self.ip_request_counts.items(), 
                                     key=lambda x: x[1], reverse=True)[:10]),
                'hourly_traffic': dict(self.hourly_traffic)
            }
        
        return report
    
    def print_dashboard(self):
        """Print real-time dashboard"""
        os.system('cls' if os.name == 'nt' else 'clear')  # Clear screen
        
        print("=" * 80)
        print("🚀 REAL-TIME WEB SERVER LOG ANALYTICS DASHBOARD")
        print("=" * 80)
        print(f"⏰ Current Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Analysis Window: {self.window_size} minutes")
        print(f"📝 Logs Processed: {len(self.log_buffer)}")
        print()
        
        # Error alerts
        error_alerts = self.detect_error_spikes()
        if error_alerts:
            print("🚨 ERROR ALERTS:")
            for alert in error_alerts:
                print(f"   ⚠️  {alert['url']}: {alert['error_count']} errors in {alert['window_minutes']} min")
        
        # IP anomaly alerts
        ip_alerts = self.detect_ip_anomalies()
        if ip_alerts:
            print("🔍 ANOMALY ALERTS:")
            for alert in ip_alerts:
                print(f"   🚨 IP {alert['ip']}: {alert['request_count']} requests in {alert['window_minutes']} min")
        
        if not error_alerts and not ip_alerts:
            print("✅ No alerts - System operating normally")
        
        print()
        
        # Top URLs
        with self.lock:
            top_urls = sorted(self.url_hit_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            if top_urls:
                print("📈 TOP URLS:")
                for url, count in top_urls:
                    print(f"   {url}: {count} hits")
        
        print()
        
        # Recent errors
        with self.lock:
            error_urls = sorted(self.error_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            if error_urls:
                print("❌ ERROR-PRONE URLS:")
                for url, count in error_urls:
                    print(f"   {url}: {count} errors")
        
        print("\nPress Ctrl+C to stop...")
    
    def analytics_worker(self):
        """Background worker for analytics and cleanup"""
        while self.running:
            try:
                # Cleanup old data
                self.cleanup_old_data()
                
                # Update dashboard
                self.print_dashboard()
                
                # Sleep for a bit
                time.sleep(5)
                
            except Exception as e:
                print(f"Analytics worker error: {e}")
                time.sleep(1)
    
    def kafka_consumer_worker(self):
        """Background worker for consuming Kafka messages"""
        consumer = LogKafkaConsumer()
        
        try:
            for message in consumer.consumer:
                if not self.running:
                    break
                    
                log_data = message.value
                self.process_log(log_data)
                
        except Exception as e:
            print(f"Kafka consumer error: {e}")
        finally:
            consumer.consumer.close()
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print("\nShutting down analytics engine...")
        self.running = False
    
    def run(self):
        """Start the real-time analytics engine"""
        print("Starting Real-Time Analytics Engine...")
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Start background workers
        kafka_thread = Thread(target=self.kafka_consumer_worker, daemon=True)
        analytics_thread = Thread(target=self.analytics_worker, daemon=True)
        
        kafka_thread.start()
        analytics_thread.start()
        
        try:
            # Keep main thread alive
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        
        print("Analytics engine stopped.")

if __name__ == "__main__":
    analyzer = RealTimeAnalyzer()
    analyzer.run()