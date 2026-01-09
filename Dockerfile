# Sử dụng Node.js LTS version làm base image
FROM node:18-alpine AS builder

# Đặt thư mục làm việc
WORKDIR /app

# Copy package.json và package-lock.json (nếu có)
COPY package*.json ./

# Cài đặt dependencies (npm ci nhanh hơn và đảm bảo build nhất quán khi có package-lock.json)
RUN npm ci --omit=dev

# Stage 2: Production image
FROM node:18-alpine

# Tạo user không phải root để chạy ứng dụng
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001

# Đặt thư mục làm việc
WORKDIR /app

# Copy dependencies từ builder stage
COPY --from=builder /app/node_modules ./node_modules

# Copy toàn bộ source code
COPY package*.json ./
COPY server.js ./
COPY index.html ./
COPY new-movies.html ./
COPY style.css ./
COPY script.js ./

# Thay đổi ownership
RUN chown -R nodejs:nodejs /app

# Chuyển sang user nodejs
USER nodejs

# Expose port   
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/api/health', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

# Chạy ứng dụng
CMD ["node", "server.js"]

