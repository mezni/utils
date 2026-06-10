FROM node:22-alpine AS base
RUN corepack enable && corepack prepare pnpm@latest --activate
WORKDIR /app

FROM base AS deps
COPY pnpm-lock.yaml pnpm-workspace.yaml package.json ./
COPY source/apps/dashboard/package.json ./source/apps/dashboard/
RUN pnpm install --frozen-lockfile --filter @borne-map/dashboard

FROM base AS build
COPY --from=deps /app/node_modules ./node_modules
COPY source/apps/dashboard/ ./source/apps/dashboard/
COPY source/packages/ ./source/packages/
RUN pnpm --filter @borne-map/dashboard build
RUN cp -r source/apps/dashboard/dist .

FROM base AS runtime
COPY --from=build /app/dist ./dist
EXPOSE 5173
CMD ["npx", "vite", "preview", "--host", "0.0.0.0", "--port", "5173"]
