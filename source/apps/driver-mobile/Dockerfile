FROM node:22-alpine AS base
RUN corepack enable && corepack prepare pnpm@latest --activate
WORKDIR /app

COPY pnpm-lock.yaml pnpm-workspace.yaml package.json ./
COPY source/apps/driver-mobile/package.json ./source/apps/driver-mobile/
RUN pnpm install --frozen-lockfile --filter @borne-map/driver-mobile

COPY source/apps/driver-mobile/ ./source/apps/driver-mobile/
COPY source/packages/ ./source/packages/

EXPOSE 8081 19000 19001 19002
CMD ["pnpm", "--filter", "@borne-map/driver-mobile", "dev"]
