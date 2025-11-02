#!/usr/bin/env python3
"""
Web Server Log Generator
Simulates Apache/NASA HTTP server logs for testing the analytics system
"""

import random
import time
from datetime import datetime
from faker import Faker
import json

fake = Faker()

class LogGenerator:
    def __init__(self):
        # More realistic URL distribution for e-commerce
        self.popular_urls = [
            '/', '/home', '/products', '/search', '/category/electronics',
            '/category/books', '/category/clothing', '/category/sports',
            '/category/home-garden', '/category/beauty', '/product/laptop-123',
            '/product/phone-456', '/product/book-789', '/deals', '/bestsellers'
        ]
        
        self.common_urls = [
            '/cart', '/checkout', '/login', '/register', '/profile', '/account',
            '/api/products', '/api/users', '/api/orders', '/api/cart', '/api/search',
            '/dashboard', '/admin', '/help', '/contact', '/about', '/privacy',
            '/terms', '/shipping', '/returns', '/track-order'
        ]
        
        self.error_urls = [
            '/broken-page', '/missing-resource', '/server-error', '/timeout',
            '/database-error', '/api/broken', '/old-product-123', '/deleted-page'
        ]
        
        # Realistic status code distribution
        self.status_codes = [
            200, 200, 200, 200, 200, 200, 200, 200,  # 80% success
            301, 302, 304,  # 15% redirects
            404, 500, 503   # 5% errors
        ]
        
        # HTTP methods with realistic distribution
        self.methods = ['GET', 'GET', 'GET', 'GET', 'GET', 'POST', 'PUT', 'DELETE']
        
        # User agents for more realism
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X)',
            'Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ]
        
        # Simulate different time zones and peak hours
        self.peak_hours = [9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
        self.off_peak_hours = [0, 1, 2, 3, 4, 5, 6, 7, 8, 22, 23]
        
    def generate_ip(self):
        """Generate realistic IP addresses"""
        return fake.ipv4()
    
    def generate_timestamp(self):
        """Generate current timestamp in Apache log format"""
        return datetime.now().strftime('[%d/%b/%Y:%H:%M:%S +0000]')
    
    def generate_log_entry(self):
        """Generate a single log entry in Apache Common Log Format"""
        ip = self.generate_ip()
        timestamp = self.generate_timestamp()
        method = random.choice(self.methods)
        
        # Realistic URL selection based on popularity
        rand = random.random()
        if rand < 0.05:  # 5% error URLs
            url = random.choice(self.error_urls)
            status_code = random.choice([404, 500, 503])
        elif rand < 0.7:  # 65% popular URLs
            url = random.choice(self.popular_urls)
            status_code = random.choice(self.status_codes)
        else:  # 30% common URLs
            url = random.choice(self.common_urls)
            status_code = random.choice(self.status_codes)
        
        protocol = "HTTP/1.1"
        
        # More realistic response sizes based on content type
        if status_code == 200:
            if '/api/' in url:
                response_size = random.randint(100, 2000)  # API responses
            elif url in ['/', '/home', '/products']:
                response_size = random.randint(5000, 15000)  # Large pages
            elif '/category/' in url:
                response_size = random.randint(3000, 8000)  # Category pages
            else:
                response_size = random.randint(500, 3000)  # Regular pages
        elif status_code in [301, 302, 304]:
            response_size = random.randint(0, 500)  # Redirects
        else:
            response_size = "-"  # Errors
        
        log_entry = f'{ip} - - {timestamp} "{method} {url} {protocol}" {status_code} {response_size}'
        return log_entry
    
    def generate_anomaly_burst(self, duration=30):
        """Generate anomalous traffic burst from single IP"""
        anomaly_ip = self.generate_ip()
        print(f"Generating anomaly burst from IP: {anomaly_ip}")
        
        end_time = time.time() + duration
        while time.time() < end_time:
            timestamp = self.generate_timestamp()
            url = random.choice(self.urls)
            log_entry = f'{anomaly_ip} - - {timestamp} "GET {url} HTTP/1.1" 200 {random.randint(200, 1000)}'
            print(log_entry)
            time.sleep(0.1)  # 10 requests per second from same IP
    
    def run_continuous(self, logs_per_second=5):
        """Generate logs continuously"""
        print("Starting continuous log generation...")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                # Normal traffic
                for _ in range(logs_per_second):
                    log_entry = self.generate_log_entry()
                    print(log_entry)
                    time.sleep(1/logs_per_second)
                
                # Occasionally generate anomaly bursts
                if random.random() < 0.05:  # 5% chance every second
                    self.generate_anomaly_burst(10)
                    
        except KeyboardInterrupt:
            print("\nLog generation stopped.")

if __name__ == "__main__":
    generator = LogGenerator()
    generator.run_continuous()