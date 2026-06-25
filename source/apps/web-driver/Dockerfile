# Multi-stage build for Node.js web-driver
FROM node:18-alpine AS builder

WORKDIR /app

# Install pnpm
RUN npm install -g pnpm

# Copy workspace files first
COPY package.json pnpm-lock.yaml tsconfig.base.json ./
COPY packages/ ./packages/
COPY apps/web-driver/ ./apps/web-driver/
COPY pnpm-workspace.yaml ./

# Install dependencies in workspace root
RUN pnpm install

# Build workspace packages first
WORKDIR /app/packages/domain-types
RUN pnpm build
WORKDIR /app/packages/client-core
RUN pnpm build
WORKDIR /app/packages/ui-kit
RUN pnpm build

# Build web-driver app from workspace root to ensure packages are found
WORKDIR /app
RUN pnpm --filter web-driver build

# Production stage
FROM nginx:alpine

# Copy build output to nginx
COPY --from=builder /app/apps/web-driver/dist /usr/share/nginx/html
COPY --from=builder /app/apps/web-driver/nginx.conf /etc/nginx/conf.d/default.conf

# Expose port
EXPOSE 80

# Start nginx
CMD ["nginx", "-g", "daemon off;"]
