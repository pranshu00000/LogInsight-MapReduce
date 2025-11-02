#!/usr/bin/env python3
"""
Big Data Generator for Web Server Logs
Creates massive datasets for realistic big data analytics demonstration
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
import random

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from log_generator.generate_logs import LogGenerator

class BigDataGenerator:
    def __init__(self):
        self.generator = LogGenerator()
        
    def generate_historical_data(self, days=30, logs_per_day=100000):
        """Generate historical data spanning multiple days"""
        total_logs = days * logs_per_day
        print(f"🚀 GENERATING BIG DATA HISTORICAL DATASET")
        print(f"📅 Time Period: {days} days")
        print(f"📊 Logs per day: {logs_per_day:,}")
        print(f"📈 Total logs: {total_logs:,}")
        print(f"💾 Estimated size: ~{(total_logs * 200) / (1024*1024):.1f} MB")
        print()
        
        # Create data directory
        os.makedirs('data/big_data', exist_ok=True)
        
        start_date = datetime.now() - timedelta(days=days)
        
        for day in range(days):
            current_date = start_date + timedelta(days=day)
            date_str = current_date.strftime('%Y-%m-%d')
            filename = f'data/big_data/logs_{date_str}.json'
            
            print(f"📅 Generating data for {date_str} ({logs_per_day:,} logs)...")
            
            with open(filename, 'w') as f:
                for hour in range(24):
                    # Simulate realistic traffic patterns
                    if hour in [9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]:
                        # Peak hours - more traffic
                        hourly_logs = int(logs_per_day * 0.06)  # 6% per peak hour
                    elif hour in [21, 22, 8]:
                        # Medium traffic
                        hourly_logs = int(logs_per_day * 0.04)  # 4% per hour
                    else:
                        # Off-peak hours
                        hourly_logs = int(logs_per_day * 0.02)  # 2% per hour
                    
                    for _ in range(hourly_logs):
                        # Generate timestamp for this specific hour
                        minute = random.randint(0, 59)
                        second = random.randint(0, 59)
                        timestamp = current_date.replace(hour=hour, minute=minute, second=second)
                        
                        log_entry = self.generator.generate_log_entry()
                        
                        # Parse and modify timestamp
                        import re
                        from config.config import LOG_FORMAT
                        
                        pattern = LOG_FORMAT['apache_common']
                        match = re.match(pattern, log_entry)
                        
                        if match:
                            log_data = {
                                'ip': match.group(1),
                                'timestamp': timestamp.strftime('%d/%b/%Y:%H:%M:%S +0000'),
                                'method': match.group(3),
                                'url': match.group(4),
                                'protocol': match.group(5),
                                'status_code': int(match.group(6)),
                                'response_size': match.group(7),
                                'raw_log': log_entry,
                                'date': date_str,
                                'hour': hour
                            }
                            f.write(json.dumps(log_data) + '\n')
            
            file_size = os.path.getsize(filename) / (1024 * 1024)
            print(f"   ✅ {filename} created ({file_size:.2f} MB)")
        
        print(f"\n🎉 BIG DATA GENERATION COMPLETE!")
        print(f"📁 Location: data/big_data/")
        print(f"📊 Total files: {days}")
        print(f"📈 Total records: {total_logs:,}")
        
        # Calculate total size
        total_size = sum(os.path.getsize(f'data/big_data/logs_{(start_date + timedelta(days=d)).strftime("%Y-%m-%d")}.json') 
                        for d in range(days)) / (1024 * 1024)
        print(f"💾 Total size: {total_size:.2f} MB")
        
        return f'data/big_data/'
    
    def generate_enterprise_dataset(self):
        """Generate enterprise-scale dataset (1M+ logs)"""
        print("🏢 GENERATING ENTERPRISE-SCALE DATASET")
        print("This simulates a major e-commerce platform like Amazon/eBay")
        print()
        
        return self.generate_historical_data(days=7, logs_per_day=200000)  # 1.4M logs
    
    def generate_startup_dataset(self):
        """Generate startup-scale dataset"""
        print("🚀 GENERATING STARTUP-SCALE DATASET")
        print("This simulates a growing e-commerce startup")
        print()
        
        return self.generate_historical_data(days=14, logs_per_day=50000)  # 700K logs
    
    def generate_demo_dataset(self):
        """Generate demo-friendly dataset"""
        print("🎯 GENERATING DEMO DATASET")
        print("Perfect size for presentations and demonstrations")
        print()
        
        return self.generate_historical_data(days=3, logs_per_day=30000)  # 90K logs

def main():
    generator = BigDataGenerator()
    
    print("🔥 BIG DATA GENERATOR FOR WEB SERVER LOGS")
    print("=" * 50)
    print()
    print("Choose dataset size:")
    print("1. Demo Dataset (90K logs, ~18MB) - Perfect for presentations")
    print("2. Startup Dataset (700K logs, ~140MB) - Growing business scale")
    print("3. Enterprise Dataset (1.4M logs, ~280MB) - Major platform scale")
    print("4. Custom Dataset - Choose your own parameters")
    print()
    
    choice = input("Enter your choice (1-4): ").strip()
    
    start_time = time.time()
    
    if choice == '1':
        generator.generate_demo_dataset()
    elif choice == '2':
        generator.generate_startup_dataset()
    elif choice == '3':
        generator.generate_enterprise_dataset()
    elif choice == '4':
        days = int(input("Enter number of days: "))
        logs_per_day = int(input("Enter logs per day: "))
        generator.generate_historical_data(days, logs_per_day)
    else:
        print("Invalid choice, generating demo dataset...")
        generator.generate_demo_dataset()
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n⏱️  Generation completed in {duration:.2f} seconds")
    print("🎉 Ready for big data analytics!")

if __name__ == "__main__":
    main()