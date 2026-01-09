// API endpoint - Lấy dữ liệu từ PostgreSQL database qua backend API
const API_URL = window.location.pathname.includes("new-movies.html")
  ? "http://localhost:3000/api/new-movies"
  : "http://localhost:3000/api/movies";
const IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500";

const moviesGrid = document.getElementById("moviesGrid");
const loading = document.getElementById("loading");
const error = document.getElementById("error");

// Hàm lấy danh sách phim
async function fetchMovies() {
  try {
    loading.style.display = "block";
    error.style.display = "none";

    const response = await fetch(API_URL);

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(
        `HTTP ${response.status}: ${
          errorText || "Không thể tải dữ liệu từ API"
        }`
      );
    }

    const data = await response.json();
    console.log("Dữ liệu nhận được:", data);

    // Backend trả về { results: [...], total: ... }
    const movies = data.results || data;

    if (!movies || movies.length === 0) {
      throw new Error("Không có dữ liệu phim. Vui lòng kiểm tra database.");
    }

    displayMovies(movies);
    loading.style.display = "none";
  } catch (err) {
    console.error("Lỗi chi tiết:", err);
    loading.style.display = "none";
    error.style.display = "block";
    error.innerHTML = `
      <p>Không thể tải danh sách phim.</p>
      <p style="font-size: 0.9rem; margin-top: 10px;">${err.message}</p>
      <p style="font-size: 0.8rem; margin-top: 10px; color: #666;">
        Kiểm tra: Server đã chạy chưa? Database đã có dữ liệu chưa?
      </p>
    `;
  }
}

// Hàm hiển thị danh sách phim
function displayMovies(movies) {
  moviesGrid.innerHTML = "";

  if (!movies || !Array.isArray(movies) || movies.length === 0) {
    moviesGrid.innerHTML = `
      <div style="grid-column: 1 / -1; text-align: center; color: white; padding: 40px;">
        <p>Không có phim nào để hiển thị.</p>
      </div>
    `;
    return;
  }

  movies.forEach((movie) => {
    const movieCard = createMovieCard(movie);
    moviesGrid.appendChild(movieCard);
  });

  console.log(`Đã hiển thị ${movies.length} phim`);
}

// Hàm tạo thẻ phim
function createMovieCard(movie) {
  const card = document.createElement("div");
  card.className = "movie-card";

  // Xử lý poster_path: có thể là URL đầy đủ hoặc đường dẫn tương đối
  let posterUrl;
  if (movie.poster_path) {
    if (movie.poster_path.startsWith("http")) {
      posterUrl = movie.poster_path;
    } else {
      posterUrl = `${IMAGE_BASE_URL}${movie.poster_path}`;
    }
  } else {
    posterUrl = "https://via.placeholder.com/500x750?text=No+Image";
  }

  const releaseDate = movie.release_date
    ? new Date(movie.release_date).getFullYear()
    : "N/A";

  const rating = movie.vote_average ? movie.vote_average.toFixed(1) : "N/A";

  card.innerHTML = `
        <img src="${posterUrl}" alt="${movie.title}" class="movie-poster" 
             onerror="this.src='https://via.placeholder.com/500x750?text=No+Image'">
        <div class="movie-info">
            <h3 class="movie-title">${movie.title}</h3>
            ${
              movie.overview
                ? `<p class="movie-overview">${movie.overview}</p>`
                : ""
            }
            ${
              movie.genres
                ? `<p class="movie-genres" style="color: #667eea; font-size: 0.8rem; margin-top: 8px;">${movie.genres}</p>`
                : ""
            }
            <div class="movie-details">
                <span class="movie-rating">⭐ ${rating}</span>
                <span class="movie-date">${releaseDate}</span>
            </div>
        </div>
    `;

  return card;
}

// Khởi chạy ứng dụng
fetchMovies();
