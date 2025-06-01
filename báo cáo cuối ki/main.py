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

