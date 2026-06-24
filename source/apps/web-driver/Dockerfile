# Multi-stage build for Node.js web-driver
FROM node:18-alpine AS builder

WORKDIR /app

# Install pnpm
RUN npm install -g pnpm

# Copy web-driver files
COPY package.json pnpm-lock.json tsconfig.json vite.config.ts vite-env.d.ts ./web-driver/

# Install dependencies
WORKDIR /app/web-driver
RUN pnpm install

# Build the application
RUN pnpm build

# Production stage
FROM nginx:alpine

# Copy build output to nginx
COPY --from=builder /app/web-driver/dist /usr/share/nginx/html
COPY --from=builder /app/web-driver/nginx.conf /etc/nginx/conf.d/default.conf

# Expose port
EXPOSE 80

# Start nginx
CMD ["nginx", "-g", "daemon off;"]
