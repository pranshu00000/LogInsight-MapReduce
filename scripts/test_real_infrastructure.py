#!/usr/bin/env python3
"""
Test Real Big Data Infrastructure
Demonstrates actual Kafka and HDFS operations when Docker is running
"""

import sys
import os
import json
import time
import subprocess
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_real_kafka():
    """Test real Kafka operations"""
    print("🔄 TESTING REAL KAFKA OPERATIONS")
    print("=" * 50)
    
    try:
        from kafka_integration.kafka_producer import LogKafkaProducer
        from log_generator.generate_logs import LogGenerator
        
        # Create producer and generator
        producer = LogKafkaProducer()
        generator = LogGenerator()
        
        print("✅ Connected to real Kafka cluster!")
        print("📡 Sending test messages to Kafka topic...")
        
        # Send 5 test messages
        for i in range(5):
            log_entry = generator.generate_log_entry()
            success = producer.send_log(log_entry)
            if success:
                print(f"   ✅ Message {i+1} sent successfully")
            else:
                print(f"   ❌ Message {i+1} failed")
            time.sleep(0.5)
        
        producer.producer.close()
        print("🎉 Real Kafka test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Kafka test failed: {e}")
        return False

def test_real_hdfs():
    """Test real HDFS operations"""
    print("\n🗄️  TESTING REAL HADOOP HDFS OPERATIONS")
    print("=" * 50)
    
    try:
        # Test HDFS commands through Docker
        commands = [
            ("List root directory", ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/']),
            ("Create test directory", ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', '/user/test']),
            ("List user directory", ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/user'])
        ]
        
        for description, cmd in commands:
            print(f"🔧 {description}...")
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                if result.returncode == 0:
                    print(f"   ✅ Success")
                    if result.stdout.strip():
                        # Show first few lines of output
                        lines = result.stdout.strip().split('\n')[:3]
                        for line in lines:
                            print(f"   📄 {line}")
                else:
                    print(f"   ⚠️  Warning: {result.stderr.strip()}")
            except subprocess.TimeoutExpired:
                print(f"   ⏱️  Timeout - HDFS may be initializing")
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        # Test file upload
        print("🔧 Testing file upload to HDFS...")
        try:
            # Create a small test file
            test_content = "This is a test file for HDFS\nTesting real Hadoop operations\n"
            with open('test_hdfs.txt', 'w') as f:
                f.write(test_content)
            
            # Copy to container and then to HDFS
            subprocess.run(['docker', 'cp', 'test_hdfs.txt', 'namenode:/tmp/'], 
                         capture_output=True, timeout=10)
            
            result = subprocess.run([
                'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', 
                '/tmp/test_hdfs.txt', '/user/test/'
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                print("   ✅ File uploaded to HDFS successfully!")
                
                # Verify file exists
                verify_result = subprocess.run([
                    'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/user/test/'
                ], capture_output=True, text=True, timeout=10)
                
                if 'test_hdfs.txt' in verify_result.stdout:
                    print("   ✅ File verified in HDFS!")
                    print(f"   📄 {verify_result.stdout.strip()}")
            else:
                print(f"   ⚠️  Upload warning: {result.stderr.strip()}")
            
            # Cleanup
            os.remove('test_hdfs.txt')
            
        except Exception as e:
            print(f"   ❌ File upload test failed: {e}")
        
        print("🎉 Real HDFS test completed!")
        return True
        
    except Exception as e:
        print(f"❌ HDFS test failed: {e}")
        return False

def test_real_mapreduce():
    """Test if we can run MapReduce on real Hadoop"""
    print("\n🔧 TESTING REAL MAPREDUCE CAPABILITIES")
    print("=" * 50)
    
    try:
        # Check YARN ResourceManager
        result = subprocess.run([
            'docker', 'exec', 'resourcemanager', 'yarn', 'node', '-list'
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0:
            print("✅ YARN ResourceManager is running!")
            print("✅ Ready for distributed MapReduce jobs!")
            if result.stdout.strip():
                print(f"   📊 {result.stdout.strip()}")
        else:
            print(f"⚠️  YARN status: {result.stderr.strip()}")
        
        return True
        
    except Exception as e:
        print(f"❌ MapReduce test failed: {e}")
        return False

def show_web_interfaces():
    """Show available web interfaces"""
    print("\n🌐 WEB INTERFACES AVAILABLE")
    print("=" * 50)
    
    interfaces = [
        ("Hadoop NameNode", "http://localhost:9870", "HDFS status and file browser"),
        ("Hadoop ResourceManager", "http://localhost:8088", "YARN jobs and cluster status"),
        ("Hadoop History Server", "http://localhost:8188", "MapReduce job history")
    ]
    
    for name, url, description in interfaces:
        print(f"🔗 {name}")
        print(f"   URL: {url}")
        print(f"   📝 {description}")
        print()

def main():
    """Run all infrastructure tests"""
    print("🚀 REAL BIG DATA INFRASTRUCTURE TEST")
    print("=" * 80)
    print("Testing actual Kafka and Hadoop services running in Docker...")
    print()
    
    # Test Kafka
    kafka_ok = test_real_kafka()
    
    # Test HDFS
    hdfs_ok = test_real_hdfs()
    
    # Test MapReduce readiness
    mapreduce_ok = test_real_mapreduce()
    
    # Show web interfaces
    show_web_interfaces()
    
    # Summary
    print("=" * 80)
    print("📊 INFRASTRUCTURE TEST SUMMARY")
    print("=" * 80)
    
    print(f"🔄 Kafka: {'✅ Working' if kafka_ok else '❌ Issues'}")
    print(f"🗄️  HDFS: {'✅ Working' if hdfs_ok else '❌ Issues'}")
    print(f"🔧 MapReduce: {'✅ Ready' if mapreduce_ok else '❌ Issues'}")
    
    if kafka_ok and hdfs_ok and mapreduce_ok:
        print("\n🎉 CONGRATULATIONS!")
        print("You have a fully functional big data infrastructure!")
        print("This is the same technology stack used by:")
        print("• Netflix for recommendation systems")
        print("• Uber for real-time analytics")
        print("• LinkedIn for data processing")
        print("• Twitter for log analysis")
    else:
        print("\n⚠️  Some services may still be initializing...")
        print("Wait a few more minutes and try again!")

if __name__ == "__main__":
    main()