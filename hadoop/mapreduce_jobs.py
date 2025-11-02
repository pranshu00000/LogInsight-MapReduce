#!/usr/bin/env python3
"""
MapReduce Jobs for Log Analysis
Implements various MapReduce jobs for analyzing web server logs
"""

import sys
import os
import json
from collections import defaultdict, Counter
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class LogAnalysisMapReduce:
    
    @staticmethod
    def error_analysis_mapper(log_line):
        """Map function for error analysis"""
        try:
            log_data = json.loads(log_line.strip())
            status_code = log_data.get('status_code', 0)
            url = log_data.get('url', '')
            
            if status_code >= 400:  # Error codes
                yield (url, status_code), 1
        except:
            pass
    
    @staticmethod
    def error_analysis_reducer(key_value_pairs):
        """Reduce function for error analysis"""
        error_counts = defaultdict(int)
        
        for (url, status_code), count in key_value_pairs:
            error_counts[(url, status_code)] += count
        
        return dict(error_counts)
    
    @staticmethod
    def peak_usage_mapper(log_line):
        """Map function for peak usage analysis"""
        try:
            log_data = json.loads(log_line.strip())
            timestamp = log_data.get('timestamp', '')
            
            # Parse timestamp and extract hour
            # Format: [25/Dec/2023:10:00:00 +0000]
            if timestamp:
                hour = timestamp.split(':')[1] if ':' in timestamp else '00'
                yield hour, 1
        except:
            pass
    
    @staticmethod
    def peak_usage_reducer(key_value_pairs):
        """Reduce function for peak usage analysis"""
        hourly_counts = defaultdict(int)
        
        for hour, count in key_value_pairs:
            hourly_counts[hour] += count
        
        return dict(hourly_counts)
    
    @staticmethod
    def ip_analysis_mapper(log_line):
        """Map function for IP analysis (anomaly detection)"""
        try:
            log_data = json.loads(log_line.strip())
            ip = log_data.get('ip', '')
            
            if ip:
                yield ip, 1
        except:
            pass
    
    @staticmethod
    def ip_analysis_reducer(key_value_pairs):
        """Reduce function for IP analysis"""
        ip_counts = defaultdict(int)
        
        for ip, count in key_value_pairs:
            ip_counts[ip] += count
        
        return dict(ip_counts)
    
    @staticmethod
    def url_popularity_mapper(log_line):
        """Map function for URL popularity analysis"""
        try:
            log_data = json.loads(log_line.strip())
            url = log_data.get('url', '')
            status_code = log_data.get('status_code', 0)
            
            if status_code == 200 and url:  # Only successful requests
                yield url, 1
        except:
            pass
    
    @staticmethod
    def url_popularity_reducer(key_value_pairs):
        """Reduce function for URL popularity analysis"""
        url_counts = defaultdict(int)
        
        for url, count in key_value_pairs:
            url_counts[url] += count
        
        return dict(url_counts)

class LogAnalyzer:
    def __init__(self):
        self.mapreduce = LogAnalysisMapReduce()
    
    def run_mapreduce_job(self, input_file, mapper, reducer):
        """Generic MapReduce job runner"""
        # Map phase
        mapped_data = []
        
        try:
            with open(input_file, 'r') as f:
                for line in f:
                    for key_value in mapper(line):
                        mapped_data.append(key_value)
        except FileNotFoundError:
            print(f"Input file {input_file} not found")
            return {}
        
        # Reduce phase
        result = reducer(mapped_data)
        return result
    
    def analyze_errors(self, input_file):
        """Analyze error patterns in logs"""
        print("🔍 Running Error Analysis MapReduce Job...")
        
        result = self.run_mapreduce_job(
            input_file,
            self.mapreduce.error_analysis_mapper,
            self.mapreduce.error_analysis_reducer
        )
        
        print(f"\n=== ERROR ANALYSIS RESULTS ({len(result)} unique error patterns) ===")
        
        # Show top 20 errors for big data feel
        top_errors = sorted(result.items(), key=lambda x: x[1], reverse=True)[:20]
        total_errors = sum(result.values())
        
        print(f"📊 Total Errors Found: {total_errors:,}")
        print(f"🔥 Top Error Patterns:")
        print("-" * 60)
        
        for i, ((url, status_code), count) in enumerate(top_errors, 1):
            percentage = (count / total_errors) * 100
            print(f"{i:2d}. {url:<30} [{status_code}] {count:>6,} ({percentage:.1f}%)")
        
        if len(result) > 20:
            print(f"... and {len(result) - 20} more error patterns")
        
        return result
    
    def analyze_peak_usage(self, input_file):
        """Analyze peak usage times"""
        print("📈 Running Peak Usage Analysis MapReduce Job...")
        
        result = self.run_mapreduce_job(
            input_file,
            self.mapreduce.peak_usage_mapper,
            self.mapreduce.peak_usage_reducer
        )
        
        print(f"\n=== PEAK USAGE ANALYSIS RESULTS ===")
        
        total_requests = sum(result.values())
        print(f"📊 Total Requests Analyzed: {total_requests:,}")
        print(f"⏰ Traffic Distribution by Hour:")
        print("-" * 50)
        
        # Sort by hour and show with bar chart
        for hour in sorted(result.keys()):
            count = result[hour]
            percentage = (count / total_requests) * 100
            bar_length = int(percentage / 2)  # Scale bar
            bar = "█" * bar_length
            print(f"{hour:>2s}:00 │{bar:<25} {count:>8,} ({percentage:>5.1f}%)")
        
        # Find peak hour
        peak_hour = max(result.items(), key=lambda x: x[1])
        print(f"\n🔥 Peak Hour: {peak_hour[0]}:00 with {peak_hour[1]:,} requests")
        
        return result
    
    def analyze_ip_anomalies(self, input_file, threshold=100):
        """Analyze IP addresses for anomalies"""
        print("🔍 Running IP Anomaly Analysis MapReduce Job...")
        
        result = self.run_mapreduce_job(
            input_file,
            self.mapreduce.ip_analysis_mapper,
            self.mapreduce.ip_analysis_reducer
        )
        
        print(f"\n=== IP ANOMALY ANALYSIS RESULTS ===")
        
        total_ips = len(result)
        total_requests = sum(result.values())
        avg_requests = total_requests / total_ips if total_ips > 0 else 0
        
        print(f"📊 Total Unique IPs: {total_ips:,}")
        print(f"📊 Total Requests: {total_requests:,}")
        print(f"📊 Average Requests per IP: {avg_requests:.1f}")
        print(f"🚨 Anomaly Threshold: {threshold} requests")
        print("-" * 60)
        
        anomalies = {ip: count for ip, count in result.items() if count > threshold}
        
        if anomalies:
            print(f"🚨 SECURITY ALERTS - {len(anomalies)} Suspicious IPs Detected:")
            for i, (ip, count) in enumerate(sorted(anomalies.items(), key=lambda x: x[1], reverse=True)[:10], 1):
                risk_level = "🔴 HIGH" if count > threshold * 3 else "🟡 MEDIUM"
                print(f"{i:2d}. {ip:<15} {count:>8,} requests {risk_level}")
            
            if len(anomalies) > 10:
                print(f"... and {len(anomalies) - 10} more suspicious IPs")
        else:
            print("✅ No anomalous IP behavior detected")
        
        return anomalies
    
    def analyze_url_popularity(self, input_file, top_n=15):
        """Analyze most popular URLs"""
        print("📊 Running URL Popularity Analysis MapReduce Job...")
        
        result = self.run_mapreduce_job(
            input_file,
            self.mapreduce.url_popularity_mapper,
            self.mapreduce.url_popularity_reducer
        )
        
        print(f"\n=== TOP {top_n} POPULAR URLS ===")
        
        total_hits = sum(result.values())
        top_urls = sorted(result.items(), key=lambda x: x[1], reverse=True)[:top_n]
        
        print(f"📊 Total Successful Requests: {total_hits:,}")
        print(f"🏆 Most Popular Content:")
        print("-" * 70)
        
        for i, (url, count) in enumerate(top_urls, 1):
            percentage = (count / total_hits) * 100
            bar_length = int(percentage * 2)  # Scale bar
            bar = "█" * min(bar_length, 30)
            print(f"{i:2d}. {url:<35} {bar:<30} {count:>8,} ({percentage:>5.1f}%)")
        
        return dict(top_urls)

if __name__ == "__main__":
    analyzer = LogAnalyzer()
    
    # Example usage with sample data
    sample_file = "sample_logs.json"
    
    if len(sys.argv) > 1:
        sample_file = sys.argv[1]
    
    print(f"Analyzing logs from: {sample_file}")
    
    # Run all analyses
    analyzer.analyze_errors(sample_file)
    analyzer.analyze_peak_usage(sample_file)
    analyzer.analyze_ip_anomalies(sample_file)
    analyzer.analyze_url_popularity(sample_file)