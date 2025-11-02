#!/usr/bin/env python3
"""
HDFS Manager for Web Server Logs
Handles storage and retrieval of logs in Hadoop HDFS
"""

import os
import sys
import json
from datetime import datetime
import subprocess

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import HADOOP_CONFIG

class HDFSManager:
    def __init__(self):
        self.hdfs_path = HADOOP_CONFIG['hdfs_path']
        self.namenode = f"{HADOOP_CONFIG['namenode_host']}:{HADOOP_CONFIG['namenode_port']}"
        
    def create_directory(self, path):
        """Create directory in HDFS"""
        try:
            cmd = f"hdfs dfs -mkdir -p {path}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"Created HDFS directory: {path}")
                return True
            else:
                print(f"Failed to create directory: {result.stderr}")
                return False
        except Exception as e:
            print(f"Error creating directory: {e}")
            return False
    
    def upload_file(self, local_path, hdfs_path):
        """Upload file to HDFS"""
        try:
            cmd = f"hdfs dfs -put {local_path} {hdfs_path}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"Uploaded {local_path} to {hdfs_path}")
                return True
            else:
                print(f"Failed to upload file: {result.stderr}")
                return False
        except Exception as e:
            print(f"Error uploading file: {e}")
            return False
    
    def store_logs_batch(self, logs, batch_id=None):
        """Store a batch of logs to HDFS"""
        if not batch_id:
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create local temp file
        temp_file = f"temp_logs_{batch_id}.json"
        
        try:
            with open(temp_file, 'w') as f:
                for log in logs:
                    f.write(json.dumps(log) + '\n')
            
            # Create HDFS directory structure
            date_path = datetime.now().strftime("%Y/%m/%d")
            hdfs_dir = f"{self.hdfs_path}/{date_path}"
            self.create_directory(hdfs_dir)
            
            # Upload to HDFS
            hdfs_file_path = f"{hdfs_dir}/logs_{batch_id}.json"
            success = self.upload_file(temp_file, hdfs_file_path)
            
            # Clean up temp file
            os.remove(temp_file)
            
            return success
            
        except Exception as e:
            print(f"Error storing logs batch: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            return False
    
    def list_files(self, path=None):
        """List files in HDFS directory"""
        if not path:
            path = self.hdfs_path
            
        try:
            cmd = f"hdfs dfs -ls {path}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"Files in {path}:")
                print(result.stdout)
                return result.stdout
            else:
                print(f"Failed to list files: {result.stderr}")
                return None
        except Exception as e:
            print(f"Error listing files: {e}")
            return None
    
    def download_file(self, hdfs_path, local_path):
        """Download file from HDFS"""
        try:
            cmd = f"hdfs dfs -get {hdfs_path} {local_path}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"Downloaded {hdfs_path} to {local_path}")
                return True
            else:
                print(f"Failed to download file: {result.stderr}")
                return False
        except Exception as e:
            print(f"Error downloading file: {e}")
            return False

if __name__ == "__main__":
    hdfs_manager = HDFSManager()
    
    # Test HDFS operations
    print("Testing HDFS operations...")
    
    # Create test directory
    hdfs_manager.create_directory("/user/test")
    
    # List files
    hdfs_manager.list_files("/user")