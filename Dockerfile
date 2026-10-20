# Atualizar o build arg para uma LTS suportada após validar testes e drivers.
ARG NODE_VERSION=20
FROM node:${NODE_VERSION}-bookworm-slim AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY tsconfig*.json ./
COPY src ./src
COPY adapters ./adapters
COPY scripts/build-spool-adapter.mjs ./scripts/build-spool-adapter.mjs
RUN npm run build

FROM node:${NODE_VERSION}-bookworm-slim AS production-dependencies
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force

FROM node:${NODE_VERSION}-bookworm-slim AS runtime
ENV NODE_ENV=production HTTP_HOST=0.0.0.0 HTTP_PORT=3000 SYNC_ENABLED=false
WORKDIR /app
COPY --from=production-dependencies --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
COPY --chown=node:node package.json ./
USER node
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 CMD ["node", "-e", "fetch('http://127.0.0.1:'+(process.env.HTTP_PORT||3000)+'/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"]
CMD ["node", "dist/main.js"]
