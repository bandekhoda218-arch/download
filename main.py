import boto3
import requests
import os
from rich.progress import (
    Progress,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
)
import logging
from botocore.exceptions import ClientError
from tqdm import tqdm
import threading
import time
from datetime import datetime

def download_file(url, filepath):
    """
    Download a video to the specified path with resume support.
    Assumes 'filepath' always includes directory (e.g., 'folder/video.mp4').
    """
    # Get file size from server
    r = requests.head(url)
    r.raise_for_status()
    total_size = int(r.headers.get("content-length", 0))
    if total_size == 0:
        raise ValueError(
            "Server did not return file size or does not support range requests."
        )

    # Resume logic
    initial_pos = os.path.getsize(filepath) if os.path.exists(filepath) else 0
    if initial_pos >= total_size:
        print(f"✅ {filepath} is already fully downloaded.")
        return

    # Setup progress bar
    progress = Progress(
        "[blue]{task.description}",
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
    )

    with progress:
        task = progress.add_task(
            f"Downloading {os.path.basename(filepath)}",
            total=total_size,
            completed=initial_pos,
        )
        headers = {"Range": f"bytes={initial_pos}-"} if initial_pos else {}

        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            mode = "ab" if initial_pos > 0 else "wb"
            with open(filepath, mode) as f:
                for chunk in r.iter_content(1024 * 2048):
                    if chunk:
                        f.write(chunk)
                        progress.advance(task, len(chunk))

    print(f"✅ Download completed: {filepath}")

class S3Uploader:
    """
    A class to handle large file uploads to S3 with progress tracking and verbose logging
    """
    
    def __init__(self, bucket_name, region_name='us-east-1', 
                 aws_access_key_id=None, aws_secret_access_key=None, endpoint_url='https://s3.ir-thr-at1.arvanstorage.ir'):
        """
        Initialize the S3 uploader
        
        Args:
            bucket_name (str): Name of the S3 bucket
            region_name (str): AWS region name
            aws_access_key_id (str): AWS access key ID (optional, uses credentials file if None)
            aws_secret_access_key (str): AWS secret access key (optional, uses credentials file if None)
        """
        # Configure logging
        self.setup_logging()
        
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(__name__)
        
        # Initialize S3 client
        try:
            if aws_access_key_id and aws_secret_access_key:
                self.s3_client = boto3.client(
                    's3',
                    region_name=region_name,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    endpoint_url=endpoint_url
                )
            else:
                # Uses credentials from ~/.aws/credentials or environment variables
                self.s3_client = boto3.client('s3', region_name=region_name,endpoint_url=endpoint_url)
            
            self.logger.info(f"✅ S3 client initialized for region: {region_name}")
            self.logger.info(f"📦 Target bucket: {bucket_name}")
            
            # Verify bucket exists
            self.s3_client.head_bucket(Bucket=bucket_name)
            self.logger.info(f"✅ Bucket '{bucket_name}' is accessible")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.logger.error(f"❌ Bucket '{bucket_name}' not found")
            elif error_code == '403':
                self.logger.error(f"❌ Access denied to bucket '{bucket_name}'")
            else:
                self.logger.error(f"❌ Error accessing bucket: {e}")
            raise
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize S3 client: {e}")
            raise
    
    def setup_logging(self):
        """Configure verbose logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def calculate_file_size(self, file_path):
        """Calculate file size in human-readable format"""
        size_bytes = os.path.getsize(file_path)
        
        # Convert to human readable format
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    
    def upload_file_simple(self, file_path, s3_key=None):
        """
        Upload a file using simple put_object (for smaller files)
        
        Args:
            file_path (str): Path to the file to upload
            s3_key (str): S3 object key (filename in S3)
        
        Returns:
            bool: True if upload successful, False otherwise
        """
        if not os.path.exists(file_path):
            self.logger.error(f"❌ File not found: {file_path}")
            return False
        
        if s3_key is None:
            s3_key = os.path.basename(file_path)
        
        file_size = self.calculate_file_size(file_path)
        self.logger.info(f"📄 File: {file_path}")
        self.logger.info(f"📏 Size: {file_size}")
        self.logger.info(f"🔑 S3 Key: {s3_key}")
        
        try:
            start_time = time.time()
            self.logger.info("🚀 Starting upload...")
            
            with open(file_path, 'rb') as file:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=file
                )
            
            upload_time = time.time() - start_time
            self.logger.info(f"✅ Upload completed successfully!")
            self.logger.info(f"⏱️  Upload time: {upload_time:.2f} seconds")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Upload failed: {e}")
            return False
    
    def upload_file_multipart(self, file_path, s3_key=None, part_size_mb=50):
        """
        Upload large file using multipart upload with progress tracking
        
        Args:
            file_path (str): Path to the file to upload
            s3_key (str): S3 object key (filename in S3)
            part_size_mb (int): Part size in MB (default: 50MB)
        
        Returns:
            bool: True if upload successful, False otherwise
        """
        if not os.path.exists(file_path):
            self.logger.error(f"❌ File not found: {file_path}")
            return False
        
        if s3_key is None:
            s3_key = os.path.basename(file_path)
        
        file_size_bytes = os.path.getsize(file_path)
        file_size_human = self.calculate_file_size(file_path)
        
        # Calculate optimal part size (must be between 5MB and 5GB)
        part_size_bytes = part_size_mb * 1024 * 1024
        if part_size_bytes < 5 * 1024 * 1024:
            part_size_bytes = 5 * 1024 * 1024
            self.logger.warning(f"⚠️  Part size adjusted to minimum 5MB")
        
        self.logger.info("=" * 60)
        self.logger.info(f"📄 FILE UPLOAD DETAILS")
        self.logger.info(f"📁 Local file: {file_path}")
        self.logger.info(f"📏 File size: {file_size_human} ({file_size_bytes:,} bytes)")
        self.logger.info(f"🔑 S3 destination: s3://{self.bucket_name}/{s3_key}")
        self.logger.info(f"⚙️  Part size: {part_size_mb}MB ({part_size_bytes:,} bytes)")
        self.logger.info("=" * 60)
        
        # Check if multipart upload is needed
        if file_size_bytes < 100 * 1024 * 1024:  # Less than 100MB
            self.logger.info("📤 File size < 100MB, using simple upload...")
            return self.upload_file_simple(file_path, s3_key)
        
        self.logger.info("🔄 Large file detected, using multipart upload...")
        
        upload_id = None
        parts = []
        
        try:
            # Step 1: Initialize multipart upload
            self.logger.info("🔧 Initializing multipart upload...")
            response = self.s3_client.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            upload_id = response['UploadId']
            self.logger.info(f"📝 Upload ID: {upload_id}")
            
            # Step 2: Calculate number of parts
            num_parts = (file_size_bytes + part_size_bytes - 1) // part_size_bytes
            self.logger.info(f"🧩 Total parts: {num_parts}")
            
            # Step 3: Upload parts
            self.logger.info("🚀 Starting part uploads...")
            start_time = time.time()
            
            # Create progress bar
            progress_bar = tqdm(
                total=file_size_bytes,
                unit='B',
                unit_scale=True,
                desc=f"Uploading {os.path.basename(file_path)}",
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
            )
            
            with open(file_path, 'rb') as file:
                for part_num in range(1, num_parts + 1):
                    # Calculate byte range for this part
                    offset = (part_num - 1) * part_size_bytes
                    bytes_to_read = min(part_size_bytes, file_size_bytes - offset)
                    
                    self.logger.debug(f"📦 Uploading part {part_num}/{num_parts} "
                                    f"(bytes {offset:,}-{offset + bytes_to_read:,})")
                    
                    # Read part data
                    file.seek(offset)
                    part_data = file.read(bytes_to_read)
                    
                    # Upload part
                    part_start_time = time.time()
                    response = self.s3_client.upload_part(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        PartNumber=part_num,
                        UploadId=upload_id,
                        Body=part_data
                    )
                    part_upload_time = time.time() - part_start_time
                    
                    # Store part info
                    parts.append({
                        'PartNumber': part_num,
                        'ETag': response['ETag']
                    })
                    
                    # Update progress bar
                    progress_bar.update(bytes_to_read)
                    print(f"*****{part_upload_time}*****\n{bytes_to_read}")
                    
                    # Log part completion
                    part_speed = bytes_to_read / (1024 * 1024 * part_upload_time) if part_upload_time > 0 else 0
                    self.logger.debug(f"✅ Part {part_num} completed in {part_upload_time:.2f}s "
                                    f"({part_speed:.2f} MB/s)")
            
            progress_bar.close()
            
            # Step 4: Complete multipart upload
            self.logger.info("🔗 Completing multipart upload...")
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            total_time = time.time() - start_time
            avg_speed = file_size_bytes / (1024 * 1024 * total_time) if total_time > 0 else 0
            
            self.logger.info("=" * 60)
            self.logger.info(f"🎉 UPLOAD COMPLETED SUCCESSFULLY!")
            self.logger.info(f"⏱️  Total time: {total_time:.2f} seconds")
            self.logger.info(f"🚀 Average speed: {avg_speed:.2f} MB/s")
            self.logger.info(f"📊 File size: {file_size_human}")
            self.logger.info(f"🧩 Parts uploaded: {num_parts}")
            self.logger.info(f"📍 S3 URI: s3://{self.bucket_name}/{s3_key}")
            self.logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Multipart upload failed: {e}")
            
            # Abort upload if it was initialized
            if upload_id:
                try:
                    self.logger.warning("🛑 Aborting multipart upload...")
                    self.s3_client.abort_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        UploadId=upload_id
                    )
                    self.logger.info("✅ Upload aborted successfully")
                except Exception as abort_error:
                    self.logger.error(f"❌ Failed to abort upload: {abort_error}")
            
            return False
    
    def upload_with_transfer_config(self, file_path, s3_key=None):
        """
        Upload file using boto3's TransferConfig for automatic multipart handling
        
        Args:
            file_path (str): Path to the file to upload
            s3_key (str): S3 object key (filename in S3)
        
        Returns:
            bool: True if upload successful, False otherwise
        """
        from boto3.s3.transfer import TransferConfig
        
        if not os.path.exists(file_path):
            self.logger.error(f"❌ File not found: {file_path}")
            return False
        
        if s3_key is None:
            s3_key = os.path.basename(file_path)
        
        file_size_human = self.calculate_file_size(file_path)
        
        self.logger.info("=" * 60)
        self.logger.info("🔄 Using TransferConfig for optimized upload")
        self.logger.info(f"📄 File: {file_path}")
        self.logger.info(f"📏 Size: {file_size_human}")
        self.logger.info(f"🔑 S3 Key: {s3_key}")
        self.logger.info("=" * 60)
        
        try:
            # Configure transfer settings
            config = TransferConfig(
                multipart_threshold=100 * 1024 * 1024,  # 100MB
                max_concurrency=10,
                multipart_chunksize=50 * 1024 * 1024,  # 50MB
                use_threads=True
            )
            
            # Create custom callback for progress
            class ProgressPercentage:
                def __init__(self, filename, logger):
                    self._filename = filename
                    self._size = float(os.path.getsize(filename))
                    self._seen_so_far = 0
                    self._lock = threading.Lock()
                    self.logger = logger
                    self.start_time = time.time()
                
                def __call__(self, bytes_amount):
                    with self._lock:
                        self._seen_so_far += bytes_amount
                        percentage = (self._seen_so_far / self._size) * 100
                        elapsed = time.time() - self.start_time
                        
                        if elapsed > 0:
                            speed = self._seen_so_far / (1024 * 1024 * elapsed)
                            self.logger.info(
                                f"📤 Progress: {percentage:.1f}% "
                                f"({self._seen_so_far:,}/{self._size:,} bytes) "
                                f"Speed: {speed:.2f} MB/s"
                            )
            
            progress_callback = ProgressPercentage(file_path, self.logger)
            
            self.logger.info("🚀 Starting upload with TransferConfig...")
            start_time = time.time()
            
            self.s3_client.upload_file(
                Filename=file_path,
                Bucket=self.bucket_name,
                Key=s3_key,
                Config=config,
                Callback=progress_callback
            )
            
            total_time = time.time() - start_time
            self.logger.info(f"✅ Upload completed in {total_time:.2f} seconds")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Upload failed: {e}")
            return False


def main():
    """
    Example usage of the S3Uploader class
    """
    # Configuration
    BUCKET_NAME = "freecad"
    FILE_PATH = "FreeCAD_1.1.1-Linux-x86_64-py311.AppImage"
    S3_KEY = "FreeCAD_1.1.1-Linux-x86_64-py311.AppImage"  # Optional: specify S3 path/filename
    # Optional: AWS credentials (if not using default credentials)
    AWS_ACCESS_KEY_ID = "0a7a9cc9-39db-40b0-8c66-cec68044286c"  # Set to your access key
    AWS_SECRET_ACCESS_KEY = "18f57485865f4e7efd40fa71f80d588ca22e4fcb6be6c0bb05de84cde06060cc"  # Set to your secret key
    REGION = "ir-thr-at1"
    endpoint_url='https://s3.ir-thr-at1.arvanstorage.ir'
    try:
        download_file("https://github.com/FreeCAD/FreeCAD/releases/download/1.1.1/FreeCAD_1.1.1-Linux-x86_64-py311.AppImage",filepath=FILE_PATH)
    except Exception as e:
        print(f"❌ Download error: {e}")

    try:
        # Initialize uploader
        uploader = S3Uploader(
            bucket_name=BUCKET_NAME,
            region_name=REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=endpoint_url
        )
        
        # Method 1: Multipart upload with custom progress bar
        print("\n" + "="*60)
        print("METHOD 1: Multipart Upload with Progress Bar")
        print("="*60)
        success = uploader.upload_file_multipart(
            file_path=FILE_PATH,
            s3_key=S3_KEY,
            part_size_mb=10  # 50MB parts
        )
        
        if success:
            print("\n✅ Upload completed successfully using Method 1!")
        else:
            print("\n❌ Upload failed with Method 1")
            
            # Try alternative method
            print("\n" + "="*60)
            print("METHOD 2: TransferConfig Upload")
            print("="*60)
            success = uploader.upload_with_transfer_config(
                file_path=FILE_PATH,
                s3_key=S3_KEY
            )
            
            if success:
                print("\n✅ Upload completed successfully using Method 2!")
            else:
                print("\n❌ All upload methods failed")
    
    except Exception as e:
        print(f"❌ Fatal error: {e}")

if __name__ == "__main__":
    main()