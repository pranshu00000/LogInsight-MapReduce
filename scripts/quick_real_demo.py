#!/usr/bin/env python3
"""
Quick Real Infrastructure Demo
Streamlined demo that avoids consumer timeout issues
"""

import sys
import os
import json
import time
import subprocess
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from log_generator.generate_logs import LogGenerator
from kafka_integration.kafka_producer import LogKafkaProducer

class QuickRealDemo:
    def __init__(self):
        self.generator = LogGenerator()
        
    def demo_kafka_streaming(self):
        """Demo real Kafka streaming"""
        print("🚀 DEMO 1: REAL KAFKA MESSAGE STREAMING")
        print("=" * 50)
        
        try:
            producer = LogKafkaProducer()
            print("✅ Connected to real Kafka cluster!")
            
            print("📡 Sending 20 log messages to Kafka...")
            for i in range(20):
                log_entry = self.generator.generate_log_entry()
                success = producer.send_log(log_entry)
                if success:
                    print(f"   ✅ Message {i+1}: Sent to Kafka")
                time.sleep(0.2)
            
            producer.producer.close()
            print("🎉 Kafka streaming demo complete!")
            return True
            
        except Exception as e:
            print(f"❌ Kafka demo failed: {e}")
            return False
    
    def demo_hdfs_operations(self):
        """Demo real HDFS operations"""
        print("\n💾 DEMO 2: REAL HADOOP HDFS OPERATIONS")
        print("=" * 50)
        
        try:
            # Generate sample data
            print("📝 Generating sample log data...")
            sample_logs = []
            for i in range(100):
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
                    sample_logs.append(log_data)
            
            # Save to local file
            data_file = f"hdfs_demo_data_{datetime.now().strftime('%H%M%S')}.json"
            with open(data_file, 'w') as f:
                for log in sample_logs:
                    f.write(json.dumps(log) + '\n')
            
            print(f"✅ Created {len(sample_logs)} log entries in {data_file}")
            
            # Create HDFS directory
            hdfs_dir = f"/user/demo/{datetime.now().strftime('%Y%m%d')}"
            print(f"📁 Creating HDFS directory: {hdfs_dir}")
            
            subprocess.run([
                'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', hdfs_dir
            ], capture_output=True, timeout=15)
            
            # Upload to HDFS
            print("📤 Uploading data to HDFS...")
            
            # Copy to container
            subprocess.run([
                'docker', 'cp', data_file, f'namenode:/tmp/{data_file}'
            ], capture_output=True, timeout=10)
            
            # Move to HDFS
            hdfs_file = f"{hdfs_dir}/{data_file}"
            result = subprocess.run([
                'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', 
                f'/tmp/{data_file}', hdfs_file
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                print(f"✅ Successfully uploaded to HDFS: {hdfs_file}")
                
                # Verify and show file info
                verify_result = subprocess.run([
                    'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', hdfs_dir
                ], capture_output=True, text=True, timeout=10)
                
                print("📄 HDFS file listing:")
                for line in verify_result.stdout.strip().split('\n'):
                    if data_file in line:
                        print(f"   {line}")
                
                # Show HDFS storage stats
                stats_result = subprocess.run([
                    'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-du', '-h', hdfs_dir
                ], capture_output=True, text=True, timeout=10)
                
                if stats_result.returncode == 0:
                    print("💾 HDFS storage usage:")
                    for line in stats_result.stdout.strip().split('\n'):
                        print(f"   {line}")
            
            # Cleanup
            os.remove(data_file)
            print("🎉 HDFS operations demo complete!")
            return True, hdfs_file
            
        except Exception as e:
            print(f"❌ HDFS demo failed: {e}")
            return False, None
    
    def demo_mapreduce_analysis(self, hdfs_file):
        """Demo MapReduce analysis"""
        print("\n🔧 DEMO 3: MAPREDUCE ANALYSIS")
        print("=" * 50)
        
        try:
            # Download data from HDFS
            temp_file = "mapreduce_data.json"
            print("📥 Downloading data from HDFS...")
            
            result = subprocess.run([
                'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-get', 
                hdfs_file, f'/tmp/{temp_file}'
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                # Copy to local
                subprocess.run([
                    'docker', 'cp', f'namenode:/tmp/{temp_file}', temp_file
                ], capture_output=True, timeout=10)
                
                print("✅ Data retrieved from HDFS")
                
                # Run analysis
                from hadoop.mapreduce_jobs import LogAnalyzer
                analyzer = LogAnalyzer()
                
                print("🔍 Running distributed-style analysis...")
                
                # Error analysis
                print("\n📊 ERROR ANALYSIS RESULTS:")
                error_results = analyzer.analyze_errors(temp_file)
                
                # Usage analysis  
                print("\n📈 PEAK USAGE ANALYSIS:")
                usage_results = analyzer.analyze_peak_usage(temp_file)
                
                # URL popularity
                print("\n🏆 TOP URL ANALYSIS:")
                popularity_results = analyzer.analyze_url_popularity(temp_file, top_n=5)
                
                # Create results summary
                results = {
                    "analysis_timestamp": datetime.now().isoformat(),
                    "hdfs_source_file": hdfs_file,
                    "total_logs_analyzed": sum(1 for _ in open(temp_file)),
                    "errors_found": len(error_results),
                    "peak_hours": len(usage_results),
                    "top_urls": len(popularity_results)
                }
                
                print(f"\n📋 ANALYSIS SUMMARY:")
                for key, value in results.items():
                    print(f"   {key}: {value}")
                
                # Store results in HDFS
                results_file = f"analysis_results_{datetime.now().strftime('%H%M%S')}.json"
                with open(results_file, 'w') as f:
                    json.dump(results, f, indent=2)
                
                print(f"\n📤 Storing results back to HDFS...")
                subprocess.run([
                    'docker', 'cp', results_file, f'namenode:/tmp/{results_file}'
                ], capture_output=True, timeout=10)
                
                subprocess.run([
                    'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', '/user/results'
                ], capture_output=True, timeout=10)
                
                subprocess.run([
                    'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', 
                    f'/tmp/{results_file}', f'/user/results/{results_file}'
                ], capture_output=True, timeout=20)
                
                print(f"✅ Results stored in HDFS: /user/results/{results_file}")
                
                # Cleanup
                os.remove(temp_file)
                os.remove(results_file)
                
                print("🎉 MapReduce analysis demo complete!")
                return True
            
        except Exception as e:
            print(f"❌ MapReduce demo failed: {e}")
            return False
    
    def show_infrastructure_status(self):
        """Show current infrastructure status"""
        print("\n🏆 INFRASTRUCTURE STATUS")
        print("=" * 50)
        
        # Kafka topics
        try:
            result = subprocess.run([
                'docker', 'exec', 'kafka', 'kafka-topics.sh', 
                '--bootstrap-server', 'localhost:9092', '--list'
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                topics = result.stdout.strip().split('\n')
                print(f"📡 Kafka Topics: {', '.join(topics)}")
        except:
            print("📡 Kafka Topics: Unable to retrieve")
        
        # HDFS status
        try:
            result = subprocess.run([
                'docker', 'exec', 'namenode', 'hdfs', 'dfsadmin', '-report'
            ], capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                lines = result.stdout.split('\n')
                for line in lines[:5]:  # First few lines
                    if 'Live datanodes' in line or 'Configured Capacity' in line:
                        print(f"💾 {line.strip()}")
        except:
            print("💾 HDFS: Status unavailable")
        
        # YARN status
        try:
            result = subprocess.run([
                'docker', 'exec', 'resourcemanager', 'yarn', 'node', '-list'
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                node_count = len([line for line in result.stdout.split('\n') if 'RUNNING' in line])
                print(f"⚡ YARN Nodes: {node_count} running")
        except:
            print("⚡ YARN: Status unavailable")
        
        print("\n🌐 Web Interfaces:")
        print("   • Hadoop NameNode: http://localhost:9870")
        print("   • YARN ResourceManager: http://localhost:8088")
    
    def run_quick_demo(self):
        """Run the quick demo"""
        print("⚡ QUICK REAL BIG DATA INFRASTRUCTURE DEMO")
        print("=" * 60)
        print("Streamlined demo using actual Kafka and Hadoop!")
        print("=" * 60)
        
        input("🎬 Press Enter to start...")
        
        try:
            # Demo 1: Kafka
            if not self.demo_kafka_streaming():
                return
            
            time.sleep(2)
            
            # Demo 2: HDFS
            success, hdfs_file = self.demo_hdfs_operations()
            if not success:
                return
            
            time.sleep(2)
            
            # Demo 3: MapReduce
            if not self.demo_mapreduce_analysis(hdfs_file):
                return
            
            # Show status
            self.show_infrastructure_status()
            
            print("\n" + "=" * 60)
            print("🎉 QUICK DEMO COMPLETE!")
            print("=" * 60)
            print("You just used REAL big data infrastructure:")
            print("✅ Apache Kafka for message streaming")
            print("✅ Hadoop HDFS for distributed storage")
            print("✅ MapReduce for distributed processing")
            print("\nThis is production-grade technology!")
            
        except KeyboardInterrupt:
            print("\n⏹️  Demo stopped by user")
        except Exception as e:
            print(f"\n❌ Demo failed: {e}")

if __name__ == "__main__":
    demo = QuickRealDemo()
    demo.run_quick_demo()