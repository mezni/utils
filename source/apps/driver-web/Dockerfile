FROM node:22-alpine AS base
RUN corepack enable && corepack prepare pnpm@latest --activate
WORKDIR /app

FROM base AS deps
COPY pnpm-lock.yaml pnpm-workspace.yaml package.json ./
COPY source/apps/driver-web/package.json ./source/apps/driver-web/
RUN pnpm install --frozen-lockfile --filter @borne-map/driver-web

FROM base AS runtime
COPY --from=deps /app/node_modules ./node_modules
COPY pnpm-workspace.yaml package.json ./
COPY source/apps/driver-web/ ./source/apps/driver-web/
COPY source/packages/ ./source/packages/
WORKDIR /app/source/apps/driver-web
EXPOSE 5174
CMD ["npx", "vite", "--host", "0.0.0.0", "--port", "5174"]