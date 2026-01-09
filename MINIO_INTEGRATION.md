# Tích hợp MinIO vào Dự án Movies

Tài liệu này mô tả cách MinIO đã được tích hợp vào dự án theo các ý tưởng đã đề xuất.

## Tổng quan

MinIO được sử dụng như một object storage để:
1. Lưu trữ ảnh (poster, backdrop) thay vì gọi trực tiếp từ TMDB
2. Lưu raw JSON data từ TMDB API trước khi transform
3. Cache API responses để giảm tải database
4. Lưu ETL logs và metrics
5. Hỗ trợ versioning và audit dữ liệu

## Cấu trúc MinIO Buckets

### Bucket: `movies-assets`

Cấu trúc thư mục:
```
movies-assets/
├── raw/                          # Raw JSON từ TMDB API
│   └── tmdb/
│       ├── now_playing/
│       │   └── YYYY-MM-DD/
│       │       └── movies.json
│       └── popular/
│           └── YYYY-MM-DD/
│               └── movies.json
├── posters/                      # Ảnh poster phim
│   └── {movie_id}{poster_path}
├── backdrops/                    # Ảnh backdrop phim
│   └── {movie_id}{backdrop_path}
├── api-cache/                    # Cached API responses
│   ├── movies/
│   │   └── latest.json
│   └── new-movies/
│       └── latest.json
└── etl-logs/                     # ETL logs và metrics
    ├── fetch_new_movies/
    │   └── {run_id}/
    │       └── YYYY-MM-DD_HH-MM-SS.json
    └── fetch_popular_movies/
        └── {run_id}/
            └── YYYY-MM-DD_HH-MM-SS.json
```

## Các tính năng đã triển khai

### 1. Lưu trữ ảnh trên MinIO (Ý tưởng #1)

**Airflow DAG:**
- Task `download_movie_images` tải ảnh từ TMDB và lưu vào MinIO
- Lưu poster và backdrop với key: `posters/{movie_id}{path}` và `backdrops/{movie_id}{path}`
- Database chỉ lưu MinIO key thay vì TMDB path

**Backend:**
- Function `convertPosterPathToUrl()` chuyển đổi MinIO key sang pre-signed URL
- Pre-signed URL có thời hạn 1 giờ
- Nếu không có trong MinIO, fallback về TMDB URL

### 2. Media Pipeline cho Raw Data (Ý tưởng #2)

**Airflow DAG:**
- Sau khi fetch từ TMDB API, lưu raw JSON vào MinIO tại `raw/tmdb/{endpoint}/{date}/movies.json`
- Có thể đọc lại từ MinIO để debug hoặc reprocess
- Lưu lịch sử dữ liệu theo ngày

### 3. Cache API Response (Ý tưởng #3)

**Airflow DAG:**
- Task `cache_api_response` lưu formatted response vào MinIO
- Lưu tại `api-cache/{endpoint}/latest.json`

**Backend:**
- Function `getCachedApiResponse()` kiểm tra cache trước khi query database
- Cache có thời hạn 1 giờ
- Giảm tải database khi có nhiều request

### 4. ETL Logs và Metrics (Ý tưởng #4)

**Airflow DAG:**
- Task `log_summary` lưu metrics vào MinIO sau mỗi lần chạy
- Log bao gồm:
  - Số lượng phim đã upsert
  - Số lượng ảnh đã tải
  - Timestamp và status
- Lưu tại `etl-logs/{dag_id}/{run_id}/{timestamp}.json`

### 5. Versioning và Audit (Ý tưởng #5)

MinIO hỗ trợ versioning tự động. Mỗi lần ETL chạy:
- Raw JSON được lưu với timestamp
- ETL logs được lưu với run_id và timestamp
- Có thể so sánh dữ liệu giữa các lần chạy

## Cấu hình

### Environment Variables

**Airflow:**
```bash
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_USE_SSL=false
```

**Backend (Node.js):**
```bash
MINIO_ENDPOINT=host.docker.internal:9000  # hoặc localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_USE_SSL=false
```

### Docker Compose

MinIO service đã được thêm vào `docker-compose-airflow.yml`:
- Port 9000: API endpoint
- Port 9001: Console UI
- Default credentials: minioadmin/minioadmin

## Cách sử dụng

### 1. Khởi động MinIO

```bash
docker-compose -f docker-compose-airflow.yml up -d minio
```

Truy cập MinIO Console: http://localhost:9001
- Username: minioadmin
- Password: minioadmin

### 2. Chạy Airflow DAGs

DAGs sẽ tự động:
- Tạo bucket `movies-assets` nếu chưa tồn tại
- Lưu raw JSON, tải ảnh, cache responses, và lưu logs

### 3. Backend tự động sử dụng MinIO

- Kiểm tra cache trước khi query database
- Tạo pre-signed URLs cho ảnh từ MinIO
- Fallback về TMDB nếu không có trong MinIO

## Lợi ích

1. **Giảm phụ thuộc TMDB**: Ảnh được cache trong MinIO, không cần gọi TMDB mỗi lần
2. **Tối ưu tốc độ**: Pre-signed URLs nhanh hơn, cache API responses giảm tải database
3. **Kiểm soát tốt hơn**: Có thể quản lý version, audit, và debug dễ dàng
4. **Scalable**: MinIO có thể scale horizontally
5. **Cost-effective**: Giảm băng thông và API calls

## Mở rộng (Ý tưởng #6)

Có thể mở rộng để thu thập dữ liệu từ nhiều nguồn:
- RottenTomatoes: `raw/rottentomatoes/...`
- IMDB: `raw/imdb/...`
- YouTube trailers: `trailers/...`

Tất cả đều lưu trong MinIO trước khi transform và load vào PostgreSQL.
