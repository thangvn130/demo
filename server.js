const express = require("express");
const { Pool } = require("pg");
const cors = require("cors");
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(".")); // Serve static files (HTML, CSS, JS)

// Kết nối PostgreSQL
const pool = new Pool({
  user: process.env.DB_USER || "postgres",
  host: process.env.DB_HOST || "localhost",
  database: process.env.DB_NAME || "demo",
  password: process.env.DB_PASSWORD || "thang2004",
  port: process.env.DB_PORT || 5432,
});

// Test connection
pool.on("connect", () => {
  console.log("Đã kết nối với PostgreSQL");
});

pool.on("error", (err) => {
  console.error("Lỗi kết nối PostgreSQL:", err);
});

// API: Lấy danh sách phim hot
app.get("/api/movies", async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 20;
    console.log(`Yêu cầu lấy ${limit} phim hot...`);
    console.log(`Kết nối database: ${process.env.DB_NAME || "demo"}`);

    // Thử query với genres trước, nếu lỗi thì dùng query đơn giản
    let query = `
      SELECT 
        m.id,
        m.title,
        m.overview,
        m.poster_path,
        m.release_date,
        m.vote_average,
        m.vote_count,
        m.popularity,
        STRING_AGG(g.name, ', ' ORDER BY g.name) AS genres
      FROM popular_movies m
      LEFT JOIN popular_movie_genres mg ON m.id = mg.movie_id
      LEFT JOIN genres g ON mg.genre_id = g.id
      GROUP BY m.id, m.title, m.overview, m.poster_path, m.release_date, m.vote_average, m.vote_count, m.popularity
      ORDER BY COALESCE(m.popularity, 0) DESC, COALESCE(m.vote_average, 0) DESC
      LIMIT $1
    `;

    let result;
    try {
      result = await pool.query(query, [limit]);
    } catch (joinError) {
      // Nếu lỗi do bảng genres không tồn tại, dùng query đơn giản
      console.log("Bảng genres không tồn tại, dùng query đơn giản...");
      query = `
        SELECT 
          id,
          title,
          overview,
          poster_path,
          release_date,
          vote_average,
          vote_count,
          popularity,
          '' AS genres
        FROM popular_movies
        ORDER BY COALESCE(popularity, 0) DESC, COALESCE(vote_average, 0) DESC
        LIMIT $1
      `;
      result = await pool.query(query, [limit]);
    }

    console.log(`Tìm thấy ${result.rows.length} phim hot`);

    // Format dữ liệu để tương thích với frontend
    const movies = result.rows.map((row) => ({
      id: row.id,
      title: row.title,
      overview: row.overview || "",
      poster_path: row.poster_path || "",
      release_date: row.release_date
        ? row.release_date.toISOString().split("T")[0]
        : null,
      vote_average: parseFloat(row.vote_average) || 0,
      vote_count: row.vote_count || 0,
      popularity: parseFloat(row.popularity) || 0,
      genres: row.genres || "",
    }));

    res.json({
      results: movies,
      total: movies.length,
    });
  } catch (error) {
    console.error("Lỗi khi lấy danh sách phim hot:", error);
    res.status(500).json({
      error: "Không thể lấy danh sách phim hot",
      message: error.message,
      details: process.env.NODE_ENV === "development" ? error.stack : undefined,
    });
  }
});

// API: Lấy danh sách phim mới ra
app.get("/api/new-movies", async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 20;
    console.log(`Yêu cầu lấy ${limit} phim mới ra...`);

    // Thử query với genres trước, nếu lỗi thì dùng query đơn giản
    let query = `
      SELECT 
        m.id,
        m.title,
        m.overview,
        m.poster_path,
        m.release_date,
        m.vote_average,
        m.vote_count,
        m.popularity,
        STRING_AGG(g.name, ', ' ORDER BY g.name) AS genres
      FROM new_movies m
      LEFT JOIN new_movie_genres mg ON m.id = mg.movie_id
      LEFT JOIN genres g ON mg.genre_id = g.id
      GROUP BY m.id, m.title, m.overview, m.poster_path, m.release_date, m.vote_average, m.vote_count, m.popularity
      ORDER BY m.release_date DESC, COALESCE(m.popularity, 0) DESC
      LIMIT $1
    `;

    let result;
    try {
      result = await pool.query(query, [limit]);
    } catch (joinError) {
      // Nếu lỗi do bảng genres không tồn tại, dùng query đơn giản
      console.log("Bảng genres không tồn tại, dùng query đơn giản...");
      query = `
        SELECT 
          id,
          title,
          overview,
          poster_path,
          release_date,
          vote_average,
          vote_count,
          popularity,
          '' AS genres
        FROM new_movies
        ORDER BY release_date DESC, COALESCE(popularity, 0) DESC
        LIMIT $1
      `;
      result = await pool.query(query, [limit]);
    }

    console.log(`Tìm thấy ${result.rows.length} phim mới ra`);

    // Format dữ liệu để tương thích với frontend
    const movies = result.rows.map((row) => ({
      id: row.id,
      title: row.title,
      overview: row.overview || "",
      poster_path: row.poster_path || "",
      release_date: row.release_date
        ? row.release_date.toISOString().split("T")[0]
        : null,
      vote_average: parseFloat(row.vote_average) || 0,
      vote_count: row.vote_count || 0,
      popularity: parseFloat(row.popularity) || 0,
      genres: row.genres || "",
    }));

    res.json({
      results: movies,
      total: movies.length,
    });
  } catch (error) {
    console.error("Lỗi khi lấy danh sách phim mới ra:", error);
    res.status(500).json({
      error: "Không thể lấy danh sách phim mới ra",
      message: error.message,
      details: process.env.NODE_ENV === "development" ? error.stack : undefined,
    });
  }
});

// API: Lấy phim theo ID
app.get("/api/movies/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const query = `
      SELECT 
        m.id,
        m.title,
        m.overview,
        m.poster_path,
        m.release_date,
        m.vote_average,
        m.vote_count,
        m.popularity,
        STRING_AGG(g.name, ', ' ORDER BY g.name) AS genres
      FROM movies m
      LEFT JOIN movie_genres mg ON m.id = mg.movie_id
      LEFT JOIN genres g ON mg.genre_id = g.id
      WHERE m.id = $1
      GROUP BY m.id, m.title, m.overview, m.poster_path, m.release_date, m.vote_average, m.vote_count, m.popularity
    `;

    const result = await pool.query(query, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Không tìm thấy phim" });
    }

    const movie = result.rows[0];
    res.json({
      id: movie.id,
      title: movie.title,
      overview: movie.overview || "",
      poster_path: movie.poster_path || "",
      release_date: movie.release_date
        ? movie.release_date.toISOString().split("T")[0]
        : null,
      vote_average: parseFloat(movie.vote_average) || 0,
      vote_count: movie.vote_count || 0,
      popularity: parseFloat(movie.popularity) || 0,
      genres: movie.genres || "",
    });
  } catch (error) {
    console.error("Lỗi khi lấy thông tin phim:", error);
    res.status(500).json({ error: "Không thể lấy thông tin phim" });
  }
});

// API: Lấy phim theo thể loại
app.get("/api/movies/genre/:genre", async (req, res) => {
  try {
    const { genre } = req.params;
    const query = `
      SELECT DISTINCT
        m.id,
        m.title,
        m.overview,
        m.poster_path,
        m.release_date,
        m.vote_average,
        m.vote_count,
        m.popularity,
        STRING_AGG(g.name, ', ' ORDER BY g.name) AS genres
      FROM movies m
      INNER JOIN movie_genres mg ON m.id = mg.movie_id
      INNER JOIN genres g ON mg.genre_id = g.id
      WHERE g.name = $1
      GROUP BY m.id, m.title, m.overview, m.poster_path, m.release_date, m.vote_average, m.vote_count, m.popularity
      ORDER BY m.popularity DESC, m.vote_average DESC
    `;

    const result = await pool.query(query, [genre]);

    const movies = result.rows.map((row) => ({
      id: row.id,
      title: row.title,
      overview: row.overview || "",
      poster_path: row.poster_path || "",
      release_date: row.release_date
        ? row.release_date.toISOString().split("T")[0]
        : null,
      vote_average: parseFloat(row.vote_average) || 0,
      vote_count: row.vote_count || 0,
      popularity: parseFloat(row.popularity) || 0,
      genres: row.genres || "",
    }));

    res.json({
      results: movies,
      total: movies.length,
    });
  } catch (error) {
    console.error("Lỗi khi lấy phim theo thể loại:", error);
    res.status(500).json({ error: "Không thể lấy phim theo thể loại" });
  }
});

// API: Lấy danh sách thể loại
app.get("/api/genres", async (req, res) => {
  try {
    const query = "SELECT id, name FROM genres ORDER BY name";
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (error) {
    console.error("Lỗi khi lấy danh sách thể loại:", error);
    res.status(500).json({ error: "Không thể lấy danh sách thể loại" });
  }
});

// Health check
app.get("/api/health", async (req, res) => {
  try {
    const result = await pool.query("SELECT NOW()");
    res.json({
      status: "OK",
      database: "Connected",
      timestamp: result.rows[0].now,
    });
  } catch (error) {
    res.status(500).json({
      status: "ERROR",
      database: "Disconnected",
      error: error.message,
    });
  }
});

// Khởi động server
app.listen(PORT, () => {
  console.log(`Server đang chạy tại http://localhost:${PORT}`);
  console.log(`API endpoint: http://localhost:${PORT}/api/movies`);
});
