
import json
import os
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any, Dict, List, Optional

import requests
from minio import Minio
from minio.error import S3Error


def get_minio_client() -> Minio:
    """Tạo MinIO client từ environment variables"""
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    use_ssl = os.getenv("MINIO_USE_SSL", "false").lower() == "true"
    
    # Parse endpoint (remove http:// if present)
    if "://" in endpoint:
        endpoint = endpoint.split("://")[1]
    
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=use_ssl,
    )
    return client


def ensure_bucket_exists(bucket_name: str) -> None:
    """Đảm bảo bucket tồn tại, nếu chưa thì tạo"""
    client = get_minio_client()
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"✅ Đã tạo bucket: {bucket_name}")
        else:
            print(f"✅ Bucket đã tồn tại: {bucket_name}")
    except S3Error as e:
        raise Exception(f"Lỗi khi tạo bucket {bucket_name}: {e}")


def save_json_to_minio(
    bucket_name: str,
    object_key: str,
    data: Dict[str, Any] | List[Dict[str, Any]],
    ensure_bucket: bool = True,
) -> str:
    """
    Lưu JSON data vào MinIO
    
    Args:
        bucket_name: Tên bucket
        object_key: Đường dẫn object (key)
        data: Dữ liệu JSON cần lưu
        ensure_bucket: Tự động tạo bucket nếu chưa tồn tại
    
    Returns:
        object_key: Đường dẫn object đã lưu
    """
    if ensure_bucket:
        ensure_bucket_exists(bucket_name)
    
    client = get_minio_client()
    json_str = json.dumps(data, ensure_ascii=False, indent=2)
    json_bytes = json_str.encode("utf-8")
    
    try:
        client.put_object(
            bucket_name,
            object_key,
            BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )
        print(f"✅ Đã lưu JSON vào MinIO: {bucket_name}/{object_key}")
        return object_key
    except S3Error as e:
        raise Exception(f"Lỗi khi lưu JSON vào MinIO: {e}")


def read_json_from_minio(bucket_name: str, object_key: str) -> Dict[str, Any] | List[Dict[str, Any]]:
    """Đọc JSON từ MinIO"""
    client = get_minio_client()
    try:
        response = client.get_object(bucket_name, object_key)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()
        return data
    except S3Error as e:
        raise Exception(f"Lỗi khi đọc JSON từ MinIO: {e}")


def download_and_save_image_to_minio(
    image_url: str,
    bucket_name: str,
    object_key: str,
    ensure_bucket: bool = True,
) -> str:
    """
    Tải ảnh từ URL và lưu vào MinIO
    
    Args:
        image_url: URL ảnh cần tải (có thể là relative path từ TMDB)
        bucket_name: Tên bucket
        object_key: Đường dẫn object trong MinIO
        ensure_bucket: Tự động tạo bucket nếu chưa tồn tại
    
    Returns:
        object_key: Đường dẫn object đã lưu
    """
    if not image_url or image_url == "":
        return ""
    
    if ensure_bucket:
        ensure_bucket_exists(bucket_name)
    
    # Nếu là relative path từ TMDB, thêm base URL
    if image_url.startswith("/"):
        image_url = f"https://image.tmdb.org/t/p/original{image_url}"
    
    try:
        # Tải ảnh
        response = requests.get(image_url, timeout=30, stream=True)
        response.raise_for_status()
        
        # Xác định content type
        content_type = response.headers.get("Content-Type", "image/jpeg")
        
        # Lưu vào MinIO
        client = get_minio_client()
        image_data = BytesIO(response.content)
        client.put_object(
            bucket_name,
            object_key,
            image_data,
            length=len(response.content),
            content_type=content_type,
        )
        print(f"✅ Đã lưu ảnh vào MinIO: {bucket_name}/{object_key}")
        return object_key
    except requests.RequestException as e:
        print(f"⚠️ Không thể tải ảnh từ {image_url}: {e}")
        return ""
    except S3Error as e:
        print(f"⚠️ Lỗi khi lưu ảnh vào MinIO: {e}")
        return ""


def save_etl_log_to_minio(
    bucket_name: str,
    dag_id: str,
    run_id: str,
    log_data: Dict[str, Any],
    ensure_bucket: bool = True,
) -> str:
    """
    Lưu ETL log/metrics vào MinIO
    
    Args:
        bucket_name: Tên bucket
        dag_id: ID của DAG
        run_id: ID của DAG run
        log_data: Dữ liệu log cần lưu
        ensure_bucket: Tự động tạo bucket nếu chưa tồn tại
    
    Returns:
        object_key: Đường dẫn object đã lưu
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    object_key = f"etl-logs/{dag_id}/{run_id}/{timestamp}.json"
    
    log_data_with_metadata = {
        "dag_id": dag_id,
        "run_id": run_id,
        "timestamp": timestamp,
        "log": log_data,
    }
    
    return save_json_to_minio(bucket_name, object_key, log_data_with_metadata, ensure_bucket)


def save_cached_api_response_to_minio(
    bucket_name: str,
    endpoint: str,
    response_data: Dict[str, Any],
    ensure_bucket: bool = True,
) -> str:
    """
    Lưu cached API response vào MinIO
    
    Args:
        bucket_name: Tên bucket
        endpoint: Tên endpoint (vd: "movies", "new-movies")
        response_data: Dữ liệu response cần cache
        ensure_bucket: Tự động tạo bucket nếu chưa tồn tại
    
    Returns:
        object_key: Đường dẫn object đã lưu
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    object_key = f"api-cache/{endpoint}/latest.json"
    
    cached_data = {
        "endpoint": endpoint,
        "timestamp": timestamp,
        "data": response_data,
    }
    
    return save_json_to_minio(bucket_name, object_key, cached_data, ensure_bucket)


def get_pre_signed_url(
    bucket_name: str,
    object_key: str,
    expires_seconds: int = 3600,
) -> str:
    """
    Tạo pre-signed URL để truy cập object trong MinIO
    
    Args:
        bucket_name: Tên bucket
        object_key: Đường dẫn object
        expires_seconds: Thời gian hết hạn (mặc định 1 giờ)
    
    Returns:
        Pre-signed URL
    """
    client = get_minio_client()
    try:
        url = client.presigned_get_object(bucket_name, object_key, expires=timedelta(seconds=expires_seconds))
        return url
    except S3Error as e:
        raise Exception(f"Lỗi khi tạo pre-signed URL: {e}")
