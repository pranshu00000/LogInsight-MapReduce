#!/usr/bin/env python3
"""
Infrastructure Explorer
Guide to exploring your real big data infrastructure
"""

import subprocess
import webbrowser
import time
import sys
import os

def show_web_interfaces():
    """Show all available web interfaces"""
    print("🌐 BIG DATA INFRASTRUCTURE WEB INTERFACES")
    print("=" * 60)
    
    interfaces = [
        {
            "name": "Hadoop NameNode",
            "url": "http://localhost:9870",
            "description": "HDFS file system browser and cluster status",
            "key_features": [
                "Browse HDFS directories and files",
                "View cluster storage capacity and usage",
                "Monitor DataNode health",
                "See block replication status"
            ]
        },
        {
            "name": "YARN ResourceManager", 
            "url": "http://localhost:8088",
            "description": "Cluster resource management and job tracking",
            "key_features": [
                "View running and completed applications",
                "Monitor cluster resource usage",
                "Track MapReduce job progress",
                "See node manager status"
            ]
        },
        {
            "name": "MapReduce History Server",
            "url": "http://localhost:8188", 
            "description": "Historical job execution details",
            "key_features": [
                "View completed MapReduce jobs",
                "Analyze job performance metrics",
                "Debug failed jobs",
                "Historical job statistics"
            ]
        }
    ]
    
    for i, interface in enumerate(interfaces, 1):
        print(f"\n{i}. 🔗 {interface['name']}")
        print(f"   URL: {interface['url']}")
        print(f"   📝 {interface['description']}")
        print("   🎯 Key Features:")
        for feature in interface['key_features']:
            print(f"      • {feature}")
    
    return interfaces

def show_hdfs_exploration_guide():
    """Guide for exploring HDFS"""
    print("\n💾 HDFS EXPLORATION GUIDE")
    print("=" * 60)
    
    print("🔍 What to Look For in NameNode Web UI (http://localhost:9870):")
    print()
    
    sections = [
        {
            "tab": "Overview",
            "what_to_see": [
                "Cluster summary with total capacity (1006.85 GB)",
                "DFS Used: Shows how much data you've stored",
                "Live DataNodes: Should show 1 active node",
                "Block Pool Used: Storage efficiency metrics"
            ]
        },
        {
            "tab": "Datanodes", 
            "what_to_see": [
                "List of active DataNodes in your cluster",
                "Storage capacity per node",
                "Last contact time (should be recent)",
                "Node health status"
            ]
        },
        {
            "tab": "Utilities → Browse the file system",
            "what_to_see": [
                "Your uploaded files in /user/demo/20251102/",
                "Analysis results in /user/results/",
                "File sizes, replication factor (3x)",
                "Block information for each file"
            ]
        }
    ]
    
    for section in sections:
        print(f"📂 {section['tab']} Tab:")
        for item in section['what_to_see']:
            print(f"   ✅ {item}")
        print()

def show_yarn_exploration_guide():
    """Guide for exploring YARN"""
    print("⚡ YARN EXPLORATION GUIDE")
    print("=" * 60)
    
    print("🔍 What to Look For in ResourceManager Web UI (http://localhost:8088):")
    print()
    
    sections = [
        {
            "tab": "Cluster",
            "what_to_see": [
                "Apps Submitted/Pending/Running/Completed counters",
                "Memory and VCore usage across cluster", 
                "Active/Lost/Unhealthy nodes",
                "Cluster resource utilization"
            ]
        },
        {
            "tab": "Applications",
            "what_to_see": [
                "List of all applications (MapReduce jobs)",
                "Application state (FINISHED/RUNNING)",
                "Resource usage per application",
                "Application logs and details"
            ]
        },
        {
            "tab": "Nodes",
            "what_to_see": [
                "NodeManager status and health",
                "Available memory and cores per node",
                "Last health update timestamp",
                "Node utilization metrics"
            ]
        }
    ]
    
    for section in sections:
        print(f"📊 {section['tab']} Tab:")
        for item in section['what_to_see']:
            print(f"   ✅ {item}")
        print()

def show_current_data_locations():
    """Show where your data is stored"""
    print("📁 YOUR DATA LOCATIONS")
    print("=" * 60)
    
    print("🔍 Files Created by Your Demo:")
    
    try:
        # Show HDFS directories
        print("\n💾 HDFS Directories:")
        result = subprocess.run([
            'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '-R', '/user'
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if line.strip():
                    print(f"   📄 {line}")
        else:
            print("   ⚠️  Unable to list HDFS files")
            
    except Exception as e:
        print(f"   ❌ Error listing HDFS: {e}")
    
    try:
        # Show Kafka topics and messages
        print("\n📡 Kafka Topics:")
        result = subprocess.run([
            'docker', 'exec', 'kafka', 'kafka-topics.sh',
            '--bootstrap-server', 'localhost:9092', '--list'
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            for topic in topics:
                if topic.strip():
                    print(f"   📨 Topic: {topic}")
                    
                    # Get topic details
                    detail_result = subprocess.run([
                        'docker', 'exec', 'kafka', 'kafka-run-class.sh',
                        'kafka.tools.GetOffsetShell',
                        '--broker-list', 'localhost:9092',
                        '--topic', topic
                    ], capture_output=True, text=True, timeout=10)
                    
                    if detail_result.returncode == 0:
                        for line in detail_result.stdout.strip().split('\n'):
                            if ':' in line:
                                partition, offset = line.split(':')[-2:]
                                print(f"      Partition {partition}: {offset} messages")
        else:
            print("   ⚠️  Unable to list Kafka topics")
            
    except Exception as e:
        print(f"   ❌ Error listing Kafka: {e}")

def show_cluster_health():
    """Show cluster health and status"""
    print("\n🏥 CLUSTER HEALTH STATUS")
    print("=" * 60)
    
    checks = [
        {
            "name": "HDFS NameNode",
            "command": ['docker', 'exec', 'namenode', 'hdfs', 'dfsadmin', '-report'],
            "success_indicators": ["Live datanodes", "Configured Capacity"]
        },
        {
            "name": "YARN ResourceManager", 
            "command": ['docker', 'exec', 'resourcemanager', 'yarn', 'node', '-list'],
            "success_indicators": ["RUNNING"]
        },
        {
            "name": "Kafka Broker",
            "command": ['docker', 'exec', 'kafka', 'kafka-broker-api-versions.sh', '--bootstrap-server', 'localhost:9092'],
            "success_indicators": ["id", "host"]
        }
    ]
    
    for check in checks:
        print(f"\n🔍 Checking {check['name']}...")
        try:
            result = subprocess.run(check['command'], capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                # Check for success indicators
                found_indicators = []
                for indicator in check['success_indicators']:
                    if indicator.lower() in result.stdout.lower():
                        found_indicators.append(indicator)
                
                if found_indicators:
                    print(f"   ✅ HEALTHY - Found: {', '.join(found_indicators)}")
                    # Show first few lines of output
                    lines = result.stdout.strip().split('\n')[:3]
                    for line in lines:
                        if line.strip():
                            print(f"      {line.strip()}")
                else:
                    print(f"   ⚠️  UNKNOWN STATUS")
            else:
                print(f"   ❌ UNHEALTHY - {result.stderr.strip()}")
                
        except subprocess.TimeoutExpired:
            print(f"   ⏱️  TIMEOUT - Service may be starting")
        except Exception as e:
            print(f"   ❌ ERROR - {e}")

def interactive_explorer():
    """Interactive exploration menu"""
    print("🔍 INTERACTIVE BIG DATA INFRASTRUCTURE EXPLORER")
    print("=" * 70)
    
    while True:
        print("\nChoose what you want to explore:")
        print("1. 🌐 Open all web interfaces in browser")
        print("2. 💾 Show HDFS exploration guide") 
        print("3. ⚡ Show YARN exploration guide")
        print("4. 📁 Show current data locations")
        print("5. 🏥 Check cluster health status")
        print("6. 📊 Show infrastructure summary")
        print("7. 🚪 Exit")
        
        choice = input("\nEnter your choice (1-7): ").strip()
        
        if choice == '1':
            interfaces = show_web_interfaces()
            print(f"\n🚀 Opening web interfaces...")
            for interface in interfaces:
                try:
                    webbrowser.open(interface['url'])
                    print(f"   ✅ Opened {interface['name']}")
                    time.sleep(1)
                except:
                    print(f"   ❌ Failed to open {interface['name']}")
            
        elif choice == '2':
            show_hdfs_exploration_guide()
            
        elif choice == '3':
            show_yarn_exploration_guide()
            
        elif choice == '4':
            show_current_data_locations()
            
        elif choice == '5':
            show_cluster_health()
            
        elif choice == '6':
            show_web_interfaces()
            show_current_data_locations()
            show_cluster_health()
            
        elif choice == '7':
            print("👋 Happy exploring!")
            break
            
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    interactive_explorer()