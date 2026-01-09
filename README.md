# Phim Hot & Phim Mới Ra App

Ứng dụng hiển thị danh sách phim hot và phim mới ra từ TMDB, sử dụng Airflow để ETL dữ liệu vào PostgreSQL, và Node.js/Express để serve API và frontend.

## Cấu trúc

- **Airflow**: ETL dữ liệu từ TMDB vào PostgreSQL
  - DAG `fetch_popular_movies`: Lấy phim phổ biến hàng ngày
  - DAG `fetch_new_movies`: Lấy phim đang chiếu hàng tuần
- **Backend**: Node.js/Express API
  - `/api/movies`: Phim hot
  - `/api/new-movies`: Phim mới ra
- **Frontend**: HTML/CSS/JS
  - `index.html`: Trang phim hot
  - `new-movies.html`: Trang phim mới ra

## Yêu cầu

- **Docker và Docker Compose** đã cài đặt
- **PostgreSQL** chạy trên máy host (localhost:5432) với:
  - User: `postgres`
  - Password: `thang2004`
  - Databases: `demo` (cho app) và `airflow` (cho Airflow)
  - Nếu chưa có, tạo databases: `createdb demo` và `createdb airflow`

## Chạy với Docker Compose

1. **Đảm bảo PostgreSQL đang chạy trên máy**

2. **Mở terminal trong thư mục project:**

   ```bash
   cd path/to/demo
   ```

3. **Chạy các services:**

   ```bash
   docker-compose up --build
   ```

4. **Truy cập:**
   - Airflow UI: http://localhost:8080 (user: admin, password: admin)
   - Phim Hot: http://localhost:3000
   - Phim Mới Ra: http://localhost:3000/new-movies.html

## Cấu hình

- **TMDB API Key**: Cần set biến môi trường `TMDB_API_KEY` trong Airflow (Variables hoặc env)
- **Database**: PostgreSQL chạy trong container, database `demo` cho app, `airflow` cho Airflow

## DAGs

- `fetch_popular_movies`: Chạy hàng ngày lúc 00:00
- `fetch_new_movies`: Chạy hàng tuần (Chủ nhật) lúc 00:00

## API Endpoints

- `GET /api/movies`: Danh sách phim hot
- `GET /api/new-movies`: Danh sách phim mới ra
- `GET /api/movies/:id`: Chi tiết phim hot theo ID
- `GET /api/movies/genre/:genre`: Phim hot theo thể loại
- `GET /api/genres`: Danh sách thể loại

## Lưu ý

- Dữ liệu được lưu trong volumes Docker, không mất khi restart
- Để trigger DAG thủ công, vào Airflow UI > DAGs > Trigger DAG
