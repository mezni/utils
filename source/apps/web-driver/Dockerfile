# Multi-stage build for Node.js web-driver
FROM node:18-alpine AS builder

WORKDIR /app

# Install pnpm
RUN npm install -g pnpm

# Copy package files
COPY web-driver/package.json web-driver/pnpm-workspace.yaml ./web-driver/
COPY web-driver/pnpm-lock.yaml ./web-driver/

# Copy workspace configuration
COPY package.json pnpm-workspace.yaml ./workspace/

# Install dependencies
WORKDIR /app
RUN pnpm install --frozen-lockfile

# Copy workspace files
COPY packages/ ./packages/
COPY apps/ ./apps/

# Build the application
WORKDIR /app/apps/web-driver
RUN pnpm build

# Production stage
FROM nginx:alpine

# Copy build output to nginx
COPY --from=builder /app/apps/web-driver/dist /usr/share/nginx/html
COPY --from=builder /app/apps/web-driver/nginx.conf /etc/nginx/conf.d/default.conf

# Expose port
EXPOSE 80

# Start nginx
CMD ["nginx", "-g", "daemon off;"]
