/**
 * MinIO client utility for Node.js backend
 */
const Minio = require("minio");
require("dotenv").config();

// Tạo MinIO client
function getMinioClient() {
  const endpoint = process.env.MINIO_ENDPOINT || "localhost:9000";
  const accessKey = process.env.MINIO_ACCESS_KEY || "minioadmin";
  const secretKey = process.env.MINIO_SECRET_KEY || "minioadmin";
  const useSSL = process.env.MINIO_USE_SSL === "true";

  // Parse endpoint (remove http:// if present)
  let host = endpoint;
  let port = 9000;

  if (endpoint.includes("://")) {
    const url = new URL(
      endpoint.includes("://") ? endpoint : `http://${endpoint}`
    );
    host = url.hostname;
    port = parseInt(url.port) || 9000;
  } else if (endpoint.includes(":")) {
    [host, port] = endpoint.split(":");
    port = parseInt(port) || 9000;
  }

  return new Minio.Client({
    endPoint: host,
    port: port,
    useSSL: useSSL,
    accessKey: accessKey,
    secretKey: secretKey,
  });
}

const minioClient = getMinioClient();

/**
 * Đảm bảo bucket tồn tại
 */
async function ensureBucketExists(bucketName) {
  try {
    const exists = await minioClient.bucketExists(bucketName);
    if (!exists) {
      await minioClient.makeBucket(bucketName, "us-east-1");
      console.log(`✅ Đã tạo bucket: ${bucketName}`);
    }
  } catch (error) {
    console.error(`Lỗi khi kiểm tra/tạo bucket ${bucketName}:`, error);
    throw error;
  }
}

/**
 * Lấy cached API response từ MinIO
 */
async function getCachedApiResponse(endpoint) {
  try {
    const objectKey = `api-cache/${endpoint}/latest.json`;
    const dataStream = await minioClient.getObject("movies-assets", objectKey);

    return new Promise((resolve, reject) => {
      const chunks = [];
      dataStream.on("data", (chunk) => chunks.push(chunk));
      dataStream.on("end", () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString());
          // Kiểm tra timestamp (cache trong 1 giờ)
          const cacheAge = Date.now() - new Date(data.timestamp).getTime();
          if (cacheAge < 3600000) {
            // 1 giờ
            resolve(data.data);
          } else {
            resolve(null); // Cache đã hết hạn
          }
        } catch (error) {
          reject(error);
        }
      });
      dataStream.on("error", reject);
    });
  } catch (error) {
    // Không có cache hoặc lỗi
    return null;
  }
}

/**
 * Tạo pre-signed URL cho ảnh
 */
async function getPresignedImageUrl(
  bucketName,
  objectKey,
  expiresInSeconds = 3600
) {
  try {
    if (!objectKey || objectKey === "") {
      return null;
    }

    // Kiểm tra object có tồn tại không
    try {
      await minioClient.statObject(bucketName, objectKey);
    } catch (statError) {
      // Object không tồn tại
      console.log(`⚠️ Object không tồn tại trong MinIO: ${objectKey}`);
      return null;
    }

    const url = await minioClient.presignedGetObject(
      bucketName,
      objectKey,
      expiresInSeconds
    );

    // Thay thế host.docker.internal bằng localhost để frontend có thể truy cập
    if (url && url.includes("host.docker.internal")) {
      return url.replace("host.docker.internal", "localhost");
    }

    return url;
  } catch (error) {
    console.error(`Lỗi khi tạo pre-signed URL cho ${objectKey}:`, error);
    return null;
  }
}

/**
 * Chuyển đổi poster_path từ MinIO key sang pre-signed URL
 * Fallback về TMDB nếu MinIO không có ảnh
 */
async function convertPosterPathToUrl(posterPath) {
  if (!posterPath || posterPath === "") {
    return null;
  }

  // Nếu đã là URL đầy đủ (từ TMDB), trả về nguyên
  if (posterPath.startsWith("http")) {
    return posterPath;
  }

  // Nếu là MinIO key (posters/...), thử tạo pre-signed URL
  if (
    posterPath.startsWith("posters/") ||
    posterPath.startsWith("backdrops/")
  ) {
    const minioUrl = await getPresignedImageUrl("movies-assets", posterPath);

    // Nếu MinIO không có ảnh, fallback về TMDB
    if (!minioUrl) {
      // Extract TMDB path từ MinIO key
      // posters/1242898/6aPy2tMgQLVz2IcifrL1Z2Q9u1t.jpg -> /6aPy2tMgQLVz2IcifrL1Z2Q9u1t.jpg
      const parts = posterPath.split("/");
      if (parts.length >= 3) {
        const tmdbPath = "/" + parts.slice(2).join("/");
        return `https://image.tmdb.org/t/p/w500${tmdbPath}`;
      }
    }

    return minioUrl;
  }

  // Nếu là relative path từ TMDB, trả về URL đầy đủ
  if (posterPath.startsWith("/")) {
    return `https://image.tmdb.org/t/p/w500${posterPath}`;
  }

  return null;
}

module.exports = {
  minioClient,
  ensureBucketExists,
  getCachedApiResponse,
  getPresignedImageUrl,
  convertPosterPathToUrl,
};
