FROM node:18-alpine AS builder

WORKDIR /app

# Copy package files
COPY apps/admin-dashboard/package*.json ./
RUN npm install

# Copy source code
COPY apps/admin-dashboard ./

# Build the application
RUN npm run build

# Production image
FROM nginx:alpine

# Copy built assets from builder
COPY --from=builder /app/dist /usr/share/nginx/html

# Copy nginx config
COPY infrastructure/docker/admin-dashboard/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
