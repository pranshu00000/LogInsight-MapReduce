#!/usr/bin/env python3
"""
Demo script to showcase the Real-Time Web Server Log Analysis system
"""

import time
import threading
import subprocess
import sys
import os
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from log_generator.generate_logs import LogGenerator
from hadoop.mapreduce_jobs import LogAnalyzer
from analytics.real_time_analyzer import RealTimeAnalyzer

class DemoRunner:
    def __init__(self):
        self.generator = LogGenerator()
        self.analyzer = LogAnalyzer()
        self.running = False
        
    def generate_sample_data(self, num_logs=100000):  # Default to 100K logs for big data feel
        """Generate sample log data for demonstration"""
        print(f"🔄 Generating {num_logs:,} sample log entries for big data analysis...")
        print("This simulates a busy e-commerce website with realistic traffic patterns...")
        
        sample_logs = []
        batch_size = 10000  # Process in batches to show progress
        
        # Ensure data directory exists
        os.makedirs('data', exist_ok=True)
        sample_file = 'data/demo_logs.json'
        
        # Generate logs in batches and write incrementally
        with open(sample_file, 'w') as f:
            for batch_start in range(0, num_logs, batch_size):
                batch_end = min(batch_start + batch_size, num_logs)
                batch_logs = []
                
                print(f"📊 Processing batch {batch_start//batch_size + 1}/{(num_logs-1)//batch_size + 1} ({batch_start:,} - {batch_end:,})")
                
                for i in range(batch_start, batch_end):
                    log_entry = self.generator.generate_log_entry()
                    
                    # Parse log entry
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
                        batch_logs.append(log_data)
                        sample_logs.append(log_data)
                
                # Write batch to file
                for log in batch_logs:
                    f.write(json.dumps(log) + '\n')
                
                # Show progress
                progress = (batch_end / num_logs) * 100
                print(f"   ✅ Progress: {progress:.1f}% ({len(sample_logs):,} logs generated)")
        
        # Calculate file size
        file_size = os.path.getsize(sample_file)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"\n🎉 BIG DATA GENERATION COMPLETE!")
        print(f"📁 File: {sample_file}")
        print(f"📊 Total Records: {len(sample_logs):,}")
        print(f"💾 File Size: {file_size_mb:.2f} MB")
        print(f"🔍 This represents {len(sample_logs)//1000} minutes of high-traffic website logs")
        
        return sample_file
    
    def run_mapreduce_demo(self, sample_file):
        """Demonstrate MapReduce analysis"""
        print("\n" + "="*70)
        print("🔧 HADOOP MAPREDUCE BIG DATA ANALYSIS")
        print("="*70)
        
        # Show file statistics
        if os.path.exists(sample_file):
            file_size = os.path.getsize(sample_file)
            file_size_mb = file_size / (1024 * 1024)
            
            # Count lines (records)
            with open(sample_file, 'r') as f:
                record_count = sum(1 for _ in f)
            
            print(f"📁 Dataset: {sample_file}")
            print(f"📊 Records: {record_count:,}")
            print(f"💾 Size: {file_size_mb:.2f} MB")
            print(f"🔍 Processing {record_count:,} log entries across distributed MapReduce jobs...")
            print()
        
        print("🚀 Starting Distributed MapReduce Jobs...")
        print()
        
        print("1️⃣ ERROR PATTERN ANALYSIS")
        self.analyzer.analyze_errors(sample_file)
        
        print("\n2️⃣ TRAFFIC PEAK ANALYSIS")
        self.analyzer.analyze_peak_usage(sample_file)
        
        print("\n3️⃣ SECURITY ANOMALY DETECTION")
        # Dynamic threshold based on data size
        threshold = max(5, record_count // 10000) if 'record_count' in locals() else 5
        self.analyzer.analyze_ip_anomalies(sample_file, threshold=threshold)
        
        print("\n4️⃣ CONTENT POPULARITY ANALYSIS")
        self.analyzer.analyze_url_popularity(sample_file)
        
        print("\n" + "="*70)
        print("✅ BIG DATA MAPREDUCE ANALYSIS COMPLETE")
        print("="*70)
    
    def simulate_real_time_logs(self, duration=60):
        """Simulate real-time log generation"""
        print(f"\n🔄 Simulating real-time logs for {duration} seconds...")
        
        end_time = time.time() + duration
        log_count = 0
        
        while time.time() < end_time and self.running:
            # Generate normal traffic
            for _ in range(5):  # 5 logs per second
                log_entry = self.generator.generate_log_entry()
                print(f"[LOG {log_count}] {log_entry}")
                log_count += 1
                time.sleep(0.2)
            
            # Occasionally generate error bursts
            if log_count % 50 == 0:  # Every 50 logs
                print("\n🚨 Generating error burst...")
                for _ in range(3):
                    error_log = self.generator.generate_log_entry()
                    # Force it to be an error by modifying
                    error_log = error_log.replace(' 200 ', ' 500 ')
                    print(f"[ERROR] {error_log}")
                    log_count += 1
                print()
        
        print(f"✅ Generated {log_count} real-time log entries")
    
    def run_interactive_demo(self):
        """Run interactive demonstration"""
        print("🚀 REAL-TIME WEB SERVER LOG ANALYSIS - INTERACTIVE DEMO")
        print("="*70)
        
        while True:
            print("\nChoose a demo option:")
            print("1. Generate sample data and run MapReduce analysis")
            print("   📊 Small dataset (10K logs) - Quick demo")
            print("   📊 Medium dataset (100K logs) - Realistic big data")
            print("   📊 Large dataset (500K logs) - Enterprise scale")
            print("2. Simulate real-time log generation")
            print("3. Show system architecture")
            print("4. Run full end-to-end demo (50K logs)")
            print("5. Exit")
            
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                print("\nChoose dataset size:")
                print("1. Small (10K logs) - Quick demo (~2MB)")
                print("2. Medium (100K logs) - Big data feel (~20MB)")
                print("3. Large (500K logs) - Enterprise scale (~100MB)")
                
                size_choice = input("Enter size choice (1-3): ").strip()
                
                if size_choice == '1':
                    sample_file = self.generate_sample_data(10000)
                elif size_choice == '2':
                    sample_file = self.generate_sample_data(100000)
                elif size_choice == '3':
                    sample_file = self.generate_sample_data(500000)
                else:
                    print("Invalid choice, using medium dataset...")
                    sample_file = self.generate_sample_data(100000)
                
                self.run_mapreduce_demo(sample_file)
                
            elif choice == '2':
                duration = input("Enter duration in seconds (default 30): ").strip()
                duration = int(duration) if duration.isdigit() else 30
                self.running = True
                try:
                    self.simulate_real_time_logs(duration)
                except KeyboardInterrupt:
                    print("\n⏹️  Simulation stopped by user")
                finally:
                    self.running = False
                    
            elif choice == '3':
                self.show_architecture()
                
            elif choice == '4':
                self.run_full_demo()
                
            elif choice == '5':
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please try again.")
    
    def show_architecture(self):
        """Display system architecture"""
        print("\n" + "="*70)
        print("🏗️  SYSTEM ARCHITECTURE")
        print("="*70)
        
        architecture = """
        ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
        │   Web Server    │───▶│  Log Generator  │───▶│  Kafka Producer │
        │   (Apache/Nginx)│    │  (Simulated)    │    │                 │
        └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                                │
                                                                ▼
        ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
        │  Real-Time      │◀───│ Kafka Consumer  │◀───│  Kafka Cluster  │
        │  Analytics      │    │                 │    │  (Topic: logs)  │
        └─────────────────┘    └─────────────────┘    └─────────────────┘
                │                                                │
                ▼                                                ▼
        ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
        │   Alerts &      │    │  Hadoop HDFS    │───▶│   MapReduce     │
        │   Dashboard     │    │   (Storage)     │    │  (Batch Jobs)   │
        └─────────────────┘    └─────────────────┘    └─────────────────┘
        
        Data Flow:
        1. 🌐 Web servers generate access logs
        2. 📡 Kafka streams logs in real-time
        3. 💾 HDFS stores logs for historical analysis
        4. ⚡ Real-time analytics detect anomalies
        5. 🔧 MapReduce runs batch analysis jobs
        6. 📊 Dashboard shows insights and alerts
        """
        
        print(architecture)
        
        print("\nKey Components:")
        print("• Kafka: High-throughput message streaming")
        print("• Hadoop HDFS: Distributed file storage")
        print("• MapReduce: Parallel batch processing")
        print("• Real-time Analytics: Immediate anomaly detection")
        print("• Dashboard: Live monitoring and alerts")
    
    def run_full_demo(self):
        """Run complete end-to-end demonstration"""
        print("\n" + "="*70)
        print("🎯 FULL END-TO-END DEMONSTRATION")
        print("="*70)
        
        print("\n📋 Demo Steps:")
        print("1. Generate sample historical data")
        print("2. Run MapReduce batch analysis")
        print("3. Simulate real-time log streaming")
        print("4. Show real-time analytics")
        
        input("\nPress Enter to start the demo...")
        
        # Step 1: Generate data
        print("\n🔄 Step 1: Generating sample data...")
        sample_file = self.generate_sample_data(50000)  # 50K logs for full demo
        
        # Step 2: Batch analysis
        print("\n🔄 Step 2: Running batch analysis...")
        self.run_mapreduce_demo(sample_file)
        
        # Step 3: Real-time simulation
        print("\n🔄 Step 3: Starting real-time simulation...")
        print("(This will run for 30 seconds - press Ctrl+C to stop early)")
        
        self.running = True
        try:
            self.simulate_real_time_logs(30)
        except KeyboardInterrupt:
            print("\n⏹️  Demo stopped by user")
        finally:
            self.running = False
        
        print("\n✅ Full demo completed!")
        print("\nIn a production environment:")
        print("• Kafka would handle millions of logs per second")
        print("• HDFS would store petabytes of historical data")
        print("• Real-time analytics would trigger immediate alerts")
        print("• MapReduce jobs would run on large clusters")

if __name__ == "__main__":
    demo = DemoRunner()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--auto':
        # Run automated demo
        demo.run_full_demo()
    else:
        # Run interactive demo
        demo.run_interactive_demo()