import os
import io
import bz2
import gcsfs
import threading
from dotenv import load_dotenv
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from collections import defaultdict

# Load .env file
from pathlib import Path
dotenv_path = Path(".env")
if dotenv_path.exists():
    load_dotenv(dotenv_path)
else:
    print("⚠ Warning: .env file not found in current directory.")

SOURCE_BUCKET = os.getenv("SOURCE_BUCKET")
DEST_BUCKET = os.getenv("DEST_BUCKET")
DEST_FILENAME = os.getenv("DEST_FILENAME", "combined_output.csv.bz2")
GCS_PREFIX = os.getenv("GCS_PREFIX", "")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "32"))  # Higher for high-RAM system
CHUNK_SIZE = 1024 * 1024 * 64  # 64MB chunks - much larger with abundant RAM
MEMORY_LIMIT_GB = int(os.getenv("MEMORY_LIMIT_GB", "80"))  # Use 80GB max, leave room for system

file_counter_lock = threading.Lock()
file_counter = 0

def debug_env():
    print("------ HIGH-RAM CONFIG ------")
    print(f"SOURCE_BUCKET:        {SOURCE_BUCKET}")
    print(f"DEST_BUCKET:          {DEST_BUCKET}")
    print(f"DEST_FILENAME:        {DEST_FILENAME}")
    print(f"GCS_PREFIX:           {GCS_PREFIX}")
    print(f"MAX_WORKERS:          {MAX_WORKERS}")
    print(f"CHUNK_SIZE:           {CHUNK_SIZE // (1024*1024)}MB")
    print(f"MEMORY_LIMIT:         {MEMORY_LIMIT_GB}GB")
    print(f"GOOGLE_APPLICATION_CREDENTIALS: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")
    print("-----------------------------\n")

def download_entire_file(fs, file_path, file_index, total_files):
    """Download entire file into memory at once"""
    global file_counter
    try:
        file_size = fs.size(file_path)
        print(f"Downloading: {file_path} ({file_size // (1024*1024)}MB)")
        
        start_time = time.time()
        with fs.open(file_path, 'rb') as f:
            file_data = f.read()  # Read entire file into memory
        
        download_time = time.time() - start_time
        throughput = (file_size / (1024*1024)) / download_time if download_time > 0 else 0
        
        with file_counter_lock:
            file_counter += 1
            print(f"✔ Downloaded: {file_path} in {download_time:.2f}s ({throughput:.1f} MB/s) [{file_counter}/{total_files}]")
            
        return file_index, file_data
        
    except Exception as e:
        print(f"❌ Error downloading {file_path}: {e}")
        raise

def combine_and_upload_high_ram():
    """Optimized for high-RAM systems - download everything to memory first"""
    debug_env()
    start_time = time.time()

    # Initialize GCS with aggressive caching
    fs = gcsfs.GCSFileSystem(
        cache_timeout=600,  # 10 minute cache
        listings_expiry_time=600,
        skip_instance_cache=False,
        requests_timeout=300,  # 5 minute timeout for large files
    )
    
    prefix_path = f"{SOURCE_BUCKET}/{GCS_PREFIX}"
    print(f"Listing files under: {prefix_path}")
    csv_files = fs.find(prefix_path)
    csv_files = [f for f in csv_files if f.endswith('.csv')]
    csv_files.sort()  # Consistent ordering
    
    if not csv_files:
        print("No CSV files found!")
        return
    
    print(f"Found {len(csv_files)} CSV files.")
    
    # Estimate total size and check memory constraints
    try:
        print("Estimating total data size...")
        sample_files = csv_files[:min(10, len(csv_files))]
        sample_size = sum(fs.size(f) for f in sample_files)
        avg_size = sample_size / len(sample_files)
        estimated_total_gb = (avg_size * len(csv_files)) / (1024**3)
        
        print(f"Estimated total size: {estimated_total_gb:.2f} GB")
        
        if estimated_total_gb > MEMORY_LIMIT_GB:
            print(f"⚠ Warning: Estimated size ({estimated_total_gb:.1f}GB) exceeds memory limit ({MEMORY_LIMIT_GB}GB)")
            print("Consider using the streaming approach or increasing MEMORY_LIMIT_GB")
            response = input("Continue anyway? (y/N): ").strip().lower()
            if response != 'y':
                return
                
    except Exception as e:
        print(f"Could not estimate size: {e}")
    
    print(f"\nDownloading all files to memory using {MAX_WORKERS} workers...\n")
    
    # Phase 1: Download all files to memory in parallel
    file_data_dict = {}
    global file_counter
    file_counter = 0
    
    download_start = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all download jobs
        futures = {
            executor.submit(download_entire_file, fs, file_path, i, len(csv_files)): i
            for i, file_path in enumerate(csv_files)
        }
        
        # Collect results as they complete
        for future in as_completed(futures):
            try:
                file_index, file_data = future.result()
                file_data_dict[file_index] = file_data
            except Exception as e:
                print(f"❌ Download failed: {e}")
                raise
    
    download_time = time.time() - download_start
    total_size_mb = sum(len(data) for data in file_data_dict.values()) / (1024*1024)
    download_throughput = total_size_mb / download_time if download_time > 0 else 0
    
    print(f"\n✔ All downloads complete!")
    print(f"Downloaded {total_size_mb:.1f} MB in {download_time:.2f}s ({download_throughput:.1f} MB/s)")
    print(f"Data now in memory: {len(file_data_dict)} files\n")
    
    # Phase 2: Compress in memory
    print("Compressing data...")
    compress_start = time.time()
    
    output_buffer = io.BytesIO()
    
    # Use highest compression level since we have the luxury of memory
    with bz2.BZ2File(output_buffer, mode='wb', compresslevel=9) as bz2_writer:
        # Write files in order
        for i in range(len(csv_files)):
            if i in file_data_dict:
                bz2_writer.write(file_data_dict[i])
                # Free memory as we go to help with compression
                del file_data_dict[i]
            else:
                print(f"⚠ Warning: Missing data for file index {i}")
    
    compress_time = time.time() - compress_start
    compressed_size_mb = output_buffer.getbuffer().nbytes / (1024 * 1024)
    compression_ratio = (total_size_mb / compressed_size_mb) if compressed_size_mb > 0 else 0
    
    print(f"✔ Compression complete in {compress_time:.2f}s")
    print(f"Compressed {total_size_mb:.1f} MB → {compressed_size_mb:.1f} MB (ratio: {compression_ratio:.1f}:1)")
    
    # Phase 3: Upload
    print(f"\nUploading to: gs://{DEST_BUCKET}/{DEST_FILENAME}")
    upload_start = time.time()
    
    output_buffer.seek(0)
    client = storage.Client()
    bucket = client.bucket(DEST_BUCKET)
    blob = bucket.blob(DEST_FILENAME)
    
    # Upload with generous timeout for large files
    blob.upload_from_file(
        output_buffer, 
        rewind=True, 
        content_type='application/x-bzip2',
        timeout=7200  # 2 hour timeout
    )
    
    upload_time = time.time() - upload_start
    upload_throughput = compressed_size_mb / upload_time if upload_time > 0 else 0
    
    total_time = time.time() - start_time
    
    print(f"✔ Upload complete in {upload_time:.2f}s ({upload_throughput:.1f} MB/s)")
    print(f"\n🎉 SUCCESS! Total time: {total_time:.2f}s")
    print(f"   Download: {download_time:.1f}s ({download_throughput:.1f} MB/s)")
    print(f"   Compress: {compress_time:.1f}s")
    print(f"   Upload: {upload_time:.1f}s ({upload_throughput:.1f} MB/s)")
    print(f"   Final: gs://{DEST_BUCKET}/{DEST_FILENAME} ({compressed_size_mb:.1f} MB)")

def combine_and_upload_streaming_chunked():
    """Alternative approach: Stream files but use larger chunks and more workers"""
    debug_env()
    start_time = time.time()

    fs = gcsfs.GCSFileSystem(
        cache_timeout=600,
        listings_expiry_time=600,
        skip_instance_cache=False
    )
    
    prefix_path = f"{SOURCE_BUCKET}/{GCS_PREFIX}"
    print(f"Listing files under: {prefix_path}")
    csv_files = fs.find(prefix_path)
    csv_files = [f for f in csv_files if f.endswith('.csv')]
    csv_files.sort()
    
    print(f"Found {len(csv_files)} CSV files.\n")

    # Use a much larger in-memory buffer before compressing
    BUFFER_SIZE = 1024 * 1024 * 512  # 512MB buffer
    accumulated_data = io.BytesIO()
    output_buffer = io.BytesIO()
    
    global file_counter
    file_counter = 0
    
    def process_file_fast(file_path):
        global file_counter
        try:
            with fs.open(file_path, 'rb') as f:
                file_data = f.read()  # Read entire file
            
            with file_counter_lock:
                file_counter += 1
                print(f"✔ Processed: {file_path} ({len(file_data) // (1024*1024)}MB) [{file_counter}/{len(csv_files)}]")
                
            return file_data
            
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")
            raise

    # Download all files in parallel
    all_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file_fast, file_path) for file_path in csv_files]
        for future in as_completed(futures):
            all_data.append(future.result())
    
    print(f"\nCompressing {len(all_data)} files...")
    
    # Compress everything at once
    with bz2.BZ2File(output_buffer, mode='wb', compresslevel=9) as bz2_writer:
        for file_data in all_data:
            bz2_writer.write(file_data)
    
    total_time = time.time() - start_time
    size_mb = output_buffer.getbuffer().nbytes / (1024 * 1024)
    
    print(f"Uploading {size_mb:.2f} MB...")
    output_buffer.seek(0)
    
    client = storage.Client()
    bucket = client.bucket(DEST_BUCKET)
    blob = bucket.blob(DEST_FILENAME)
    blob.upload_from_file(output_buffer, rewind=True, content_type='application/x-bzip2')

    print(f"✔ Complete! Total time: {total_time:.2f}s")
    print(f"✔ Final: gs://{DEST_BUCKET}/{DEST_FILENAME} ({size_mb:.2f} MB)")

if __name__ == '__main__':
    # With 124GB RAM, use the high-RAM optimized version
    combine_and_upload_high_ram()
    
    # Alternative: uncomment for streaming with large chunks
    # combine_and_upload_streaming_chunked()
