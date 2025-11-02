#!/usr/bin/env python3
"""
Compare Demo Mode vs Production Mode
Shows the difference between simulated and real big data infrastructure
"""

import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_kafka_connection():
    """Test if real Kafka is available"""
    try:
        from kafka_integration.kafka_producer import LogKafkaProducer
        producer = LogKafkaProducer()
        # Try to send a test message
        test_log = {
            'ip': '127.0.0.1',
            'timestamp': '01/Nov/2025:18:00:00 +0000',
            'method': 'GET',
            'url': '/test',
            'protocol': 'HTTP/1.1',
            'status_code': 200,
            'response_size': '1024'
        }
        future = producer.producer.send('web_server_logs', value=test_log)
        future.get(timeout=5)  # Wait for confirmation
        producer.producer.close()
        return True, "✅ Real Kafka cluster is running"
    except Exception as e:
        return False, f"❌ Kafka not available: {str(e)}"

def test_hadoop_connection():
    """Test if real Hadoop is available"""
    try:
        import subprocess
        # Check if namenode container is running and accessible
        result = subprocess.run([
            'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/'
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0:
            return True, "✅ Real Hadoop HDFS is running"
        else:
            return False, f"❌ HDFS not available: {result.stderr}"
    except subprocess.TimeoutExpired:
        return False, "❌ Hadoop timeout - container may be starting"
    except Exception as e:
        # Try alternative check - see if namenode container exists
        try:
            check_result = subprocess.run([
                'docker', 'ps', '--filter', 'name=namenode', '--format', '{{.Names}}'
            ], capture_output=True, text=True, timeout=5)
            
            if 'namenode' in check_result.stdout:
                return True, "✅ Hadoop containers running (HDFS initializing)"
            else:
                return False, f"❌ Hadoop containers not found: {str(e)}"
        except:
            return False, f"❌ Hadoop not available: {str(e)}"

def show_comparison():
    """Show comparison between demo and production modes"""
    
    print("=" * 80)
    print("🔍 BIG DATA INFRASTRUCTURE COMPARISON")
    print("=" * 80)
    
    print("\n📊 TESTING CURRENT INFRASTRUCTURE...")
    
    # Test Kafka
    kafka_available, kafka_msg = test_kafka_connection()
    print(f"\n🔄 Apache Kafka: {kafka_msg}")
    
    # Test Hadoop
    hadoop_available, hadoop_msg = test_hadoop_connection()
    print(f"🗄️  Hadoop HDFS: {hadoop_msg}")
    
    print("\n" + "=" * 80)
    
    if kafka_available and hadoop_available:
        print("🚀 PRODUCTION MODE - Real Big Data Infrastructure")
        print("=" * 80)
        print("✅ You have the full big data stack running!")
        print("✅ Kafka cluster handling real message streaming")
        print("✅ Hadoop HDFS providing distributed storage")
        print("✅ YARN managing distributed computing resources")
        print("\n🎯 This is what companies like Netflix/Amazon use!")
        
        print("\n📈 CAPABILITIES IN PRODUCTION MODE:")
        print("• Handle millions of logs per second")
        print("• Store petabytes of data across clusters")
        print("• Process data in parallel across hundreds of nodes")
        print("• Provide fault tolerance and high availability")
        
    else:
        print("🎓 DEMO MODE - Simulated Big Data Processing")
        print("=" * 80)
        print("📚 You're running in educational/demo mode")
        print("🔧 Using Python simulations instead of real infrastructure")
        print("💡 Perfect for learning concepts without complexity")
        
        print("\n📊 CURRENT DEMO CAPABILITIES:")
        print("• Simulated log generation and processing")
        print("• Local file-based 'MapReduce' jobs")
        print("• Real-time analytics without message queues")
        print("• Educational value without infrastructure overhead")
        
        print("\n🚀 TO ENABLE PRODUCTION MODE:")
        print("1. Run: docker-compose up -d")
        print("2. Wait 2-3 minutes for services to start")
        print("3. Run this script again to see the difference!")
        
    print("\n" + "=" * 80)
    print("🎯 ARCHITECTURE COMPARISON")
    print("=" * 80)
    
    print("\n📚 DEMO MODE ARCHITECTURE:")
    print("Log Generator → Python Processing → Local Files → Console Output")
    
    print("\n🏭 PRODUCTION MODE ARCHITECTURE:")
    print("Log Generator → Kafka Producer → Kafka Cluster → Kafka Consumer")
    print("                                      ↓")
    print("Real-time Analytics ← Stream Processing ← Message Queue")
    print("                                      ↓")
    print("Historical Analysis ← MapReduce Jobs ← Hadoop HDFS")
    
    print("\n💡 Both modes teach the same concepts, but production mode")
    print("   shows how it works at scale with real infrastructure!")

if __name__ == "__main__":
    show_comparison()