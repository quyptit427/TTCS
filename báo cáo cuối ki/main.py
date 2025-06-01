# **IP Location Processing** 
#   - Install ip2location-python library
#    - Write Python script to:
#        
#        ```python
#       python
#        Copy
        # Pseudocode structure
#        def process_ip_locations():
        # 1. Connect to MongoDB
        # 2. Read unique IPs from main collection
        # 3. Use ip2location to get location data
        # 4. Store results in new collection Or xu
        
        ```
        
 #   - Create new MongoDB collection for location data
 #  - Implement error handling
 #   - Test with sample data

from pymongo import MongoClient
import IP2Location
import logging
import os
from datetime import datetime

# === CONFIG ===
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "countly"
SOURCE_COLLECTION = "summary"
TARGET_COLLECTION = "ip_location4"
IP2LOCATION_BIN = "/home/quy/Downloads/IP2LOCATION-LITE-DB1.BIN"
BATCH_SIZE = 500_000
LOG_PATH = "/home/quy/ip_location1_batch.log"

# === Khởi tạo logging ===
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# === Kết nối DB ===
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
source_col = db[SOURCE_COLLECTION]
target_col = db[TARGET_COLLECTION]

# === Load IP2Location ===
ip2loc = IP2Location.IP2Location(IP2LOCATION_BIN)

# === Hàm chia batch ===
def batch_iterator(cursor, batch_size):
    batch = []
    for doc in cursor:
        batch.append(doc["_id"])
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

# === Lấy danh sách IP duy nhất bằng aggregate ===
pipeline = [{"$group": {"_id": "$ip"}}]
cursor = source_col.aggregate(pipeline, allowDiskUse=True)

total_inserted = 0
for i, ip_batch in enumerate(batch_iterator(cursor, BATCH_SIZE)):
    logger.info(f"Processing batch {i+1}, size: {len(ip_batch)}")

    records = []
    for ip in ip_batch:
        try:
            loc = ip2loc.get_all(ip)
            records.append({
                "ip": ip,
                "country_short": loc.country_short,
                "country_long": loc.country_long
            })
        except Exception as e:
            logger.warning(f"Error with IP {ip}: {e}")

    if records:
        try:
            target_col.insert_many(records, ordered=False)
            logger.info(f"Inserted {len(records)} records to {TARGET_COLLECTION}")
            total_inserted += len(records)
        except Exception as e:
            logger.error(f"Insert error in batch {i+1}: {e}")

logger.info(f"✅ Finished: Total inserted records = {total_inserted}")
print(f"✅ Ghi log vào: {LOG_PATH}")
#6. **Product name collection (1 day)**
#    - Filter data in collections equal to `view_product_detail`, `select_product_option`, and `select_product_option_quality`.
#    - Get the `product_id` and `current_url` values.
#    - Crawl the product name based on the information above; get **only one active `product name`** for each distinct `product_id`.
#    - Store the data in CSV file(s) for later transformation.
import csv
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pandas as pd

# Cấu hình
BATCH_SIZE = 500
MAX_RECORDS = 1_000_000
CSV_OUTPUT = 'product_data.csv'

# Kết nối MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['countly']
collection = db['summary']

# Truy vấn lọc
query = {
    "collection": {"$in": ["view_product_detail", "select_product_option", "select_product_option_quality"]},
    "product_id": {"$exists": True, "$ne": ""},
    "current_url": {"$exists": True, "$ne": ""}
}

# Crawl product name từ URL
def get_product_name(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            title_tag = soup.find("h1", class_="product-name") or soup.find("title")
            return title_tag.text.strip() if title_tag else None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return None

# Tổng số bản ghi trong MongoDB (chỉ để báo cáo)
total_records = collection.count_documents(query)
print(f"Tổng số bản ghi phù hợp: {total_records}")

# Khởi tạo
seen_product_ids = set()
total_collected = 0

# Batch loop
for offset in range(0, total_records, BATCH_SIZE):
    if total_collected >= MAX_RECORDS:
        print(f"Đã đạt giới hạn {MAX_RECORDS} bản ghi. Kết thúc.")
        break

    print(f"🧩 Đang xử lý batch {offset} -> {offset + BATCH_SIZE}")
    docs = collection.find(query).skip(offset).limit(BATCH_SIZE)
    batch_data = []

    for doc in docs:
        pid = doc.get("product_id")
        url = doc.get("current_url")

        if pid not in seen_product_ids:
            product_name = get_product_name(url)
            if product_name:
                batch_data.append({
                    "product_id": pid,
                    "current_url": url,
                    "product_name": product_name
                })
                seen_product_ids.add(pid)
                total_collected += 1

                if total_collected >= MAX_RECORDS:
                    print("⛔ Đã đạt giới hạn 1 triệu bản ghi trong batch hiện tại.")
                    break

    # Ghi ra file CSV (append)
    if batch_data:
        df = pd.DataFrame(batch_data)
        df.to_csv(CSV_OUTPUT, mode='a', header=not pd.io.common.file_exists(CSV_OUTPUT), index=False, quoting=csv.QUOTE_ALL)

print(f"✅ Hoàn tất! Tổng số sản phẩm đã lưu: {total_collected}")
### Detailed Steps:

# **Data Export Process** 
#    - Create Python script to:
#        
#        ```python
#        
# Pseudocode structure
#       def export_to_gcs():
        # 1. Connect to MongoDB (or VM)
        # 2. Extract data in batches
        # 3. Convert to appropriate format (CSV/JSONL/JSON/PARQUET)
        # 4. Upload to GCS (all data in VM or in MongoDB)
        # 5. Log operations
        
        ```
        
    - Implement error handling
    - Add logging functionality
    - Test with sample data
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, BulkWriteError
import IP2Location
import pandas as pd
import os
from datetime import datetime
import boto3

class Logger:
    def __init__(self, filename):
        import logging
        logging.basicConfig(
            filename=filename,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger()
    
    def info(self, msg):
        self.logger.info(msg)
    
    def warning(self, msg):
        self.logger.warning(msg)
    
    def error(self, msg):
        self.logger.error(msg)
    
    def log_errors(self, logger, error_type=None):
        def decorator(func):
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if error_type is None or isinstance(e, error_type):
                        logger.error(f"Error in {func.__name__}: {e}")
                    else:
                        raise
            return wrapper
        return decorator

def batch_iterator(cursor, batch_size):
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

log_filename = "/home/quy/ip_location.log"
logger = Logger(log_filename)

# Cấu hình MinIO
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "7CHpYfc6MCv4vQyfhwB3"
MINIO_SECRET_KEY = "WZQkE2OlVBD9I8eK6lBrcp0mImCgERnc9SgA0QLv"
MINIO_BUCKET = "glamira"

def upload_file_to_minio(file_path, bucket_name, object_name):
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    buckets = s3_client.list_buckets()
    if bucket_name not in [b['Name'] for b in buckets['Buckets']]:
        s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"Created bucket: {bucket_name}")

    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info(f"Uploaded '{file_path}' as '{object_name}' to bucket '{bucket_name}'")
    except Exception as e:
        logger.error(f"Upload failed: {e}")

class ETL():
    def __init__(self):
        self.mongo_uri = "mongodb://localhost:27017/"
        self.database_name = "countly"
        self.source_collection_name = "summary"
        self.target_collection_name = "ip_location2"
        self.location_db_path = "/home/quy/Downloads/IP2LOCATION-LITE-DB1.IPV6.BIN/IP2LOCATION-LITE-DB1.IPV6.BIN"
        self.location_query_db = None
        self.remote_db = None
        self.base_collection = None
        
    def create_collection(self, collection_name=None, index=None):
        if self.remote_db is None:
            self.conn_db()
        if collection_name is None:
            collection_name = self.target_collection_name
        if collection_name not in self.remote_db.list_collection_names():
            self.remote_db.create_collection(collection_name)
            if index:
                self.remote_db[collection_name].create_index(index, unique=True)
        return self.remote_db[collection_name]
    
    @logger.log_errors(logger, error_type=ConnectionFailure)
    def conn_db(self):
        client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        remote_db = client[self.database_name]
        base_collection = remote_db[self.source_collection_name]
        self.remote_db = remote_db
        self.base_collection = base_collection
        logger.info("Connected to MongoDB")
    
    @logger.log_errors(logger)
    def extract(self, batch_size=500_000, output_prefix="ips_batch"):
        if self.base_collection is None:
            self.conn_db()

        pipeline = [{"$group": {"_id": "$ip"}}]
        cursor = self.base_collection.aggregate(pipeline, allowDiskUse=True)

        os.makedirs("data", exist_ok=True)
        for i, batch in enumerate(batch_iterator(cursor, batch_size)):
            ips = [doc["_id"] for doc in batch]
            df = pd.DataFrame(ips, columns=["ip"])

            filename = f"data/{output_prefix}_{i+1}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"Wrote {len(df)} IPs to {filename}")
         
    @logger.log_errors(logger)
    def transform(self, input_dir="data", input_prefix="ips_batch_", output_prefix="location_batch_", skip_exist=True):
        if self.location_query_db is None:
            self.load_location_query_db()

        os.makedirs(input_dir, exist_ok=True)
        for filename in sorted(os.listdir(input_dir)):
            if filename.startswith(input_prefix) and filename.endswith(".csv"):
                input_path = os.path.join(input_dir, filename)
                output_filename = filename.replace(input_prefix, output_prefix)
                output_path = os.path.join(input_dir, output_filename)

                if skip_exist and os.path.exists(output_path):
                    logger.info(f"Skipping already processed file: {output_filename}")
                    continue

                self.transform_batch(input_path, output_path)

    @logger.log_errors(logger)
    def transform_batch(self, input_path, output_path):
        logger.info(f"Processing: {input_path}")
        df = pd.read_csv(input_path)

        records = []
        for ip in df["ip"]:
            location_obj = self.location_query_db.get_all(ip)
            location_data = {
                'ip': ip,
                'country_short': location_obj.country_short,
                'country_long': location_obj.country_long
            }
            records.append(location_data)

        output_df = pd.DataFrame(records)
        output_df.to_csv(output_path, index=False)
        logger.info(f"Saved transformed data to {output_path}")
    
    @logger.log_errors(logger)
    def load(self, input_dir="data", input_prefix="location_batch_", load_collection=None):
        if load_collection is None:
            load_collection = self.target_collection_name
        load_collection_ = self.create_collection(collection_name=load_collection, index='ip')
        
        for filename in os.listdir(input_dir):
            if filename.startswith(input_prefix) and filename.endswith(".csv"):
                input_path = os.path.join(input_dir, filename)
                self.load_batch(input_path, load_collection_)
                # Upload file csv đã load lên MinIO
                upload_file_to_minio(input_path, MINIO_BUCKET, filename)

    @logger.log_errors(logger)
    def load_batch(self, input_path, load_collection):
        logger.info(f"Loading file into MongoDB: {input_path}")
        df = pd.read_csv(input_path)

        if df.empty:
            logger.warning(f"Empty CSV file: {input_path}")
            return

        records = df.to_dict(orient="records")

        batch_size = 10_000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                load_collection.insert_many(batch, ordered=False)
                logger.info(f"Inserted a small batch of {len(batch)} records from {input_path}")
            except BulkWriteError as bwe:
                logger.warning(f"Some records failed to insert: {bwe.details}")
    
    def load_location_query_db(self):
        self.location_query_db = IP2Location.IP2Location(self.location_db_path)
    
    def run(self):
        self.conn_db()
        self.extract(batch_size=500_000, output_prefix="ips_batch")
        self.transform(input_dir="data", input_prefix="ips_batch_", output_prefix="location_batch_", skip_exist=True)
        self.load(input_dir="data", input_prefix="location_batch_", load_collection=self.target_collection_name)


if _name_ == '_main_':
    etl = ETL()
    etl.run()
