import boto3
import os

# Cấu hình MinIO (thay đổi theo môi trường của bạn)
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
    
    # Tạo bucket nếu chưa có (có thể bị lỗi nếu bucket tồn tại)
    buckets = s3_client.list_buckets()
    if bucket_name not in [b['Name'] for b in buckets['Buckets']]:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")

    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Uploaded '{file_path}' as '{object_name}' to bucket '{bucket_name}'")
    except Exception as e:
        print(f"Upload failed: {e}")

if name == "main":
    # File bạn muốn upload (phải tồn tại)
    test_file = "test_upload.txt"

    # Tạo file test nhỏ
    with open(test_file, "w") as f:
        f.write("Hello MinIO! Đây là file test.\n")

    # Upload file lên MinIO
    upload_file_to_minio(test_file, MINIO_BUCKET, test_file)

    # Xóa file test sau upload (tuỳ chọn)
    os.remove(test_file)
