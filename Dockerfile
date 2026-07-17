# --- build stage -----------------------------------------------------
FROM node:22-slim AS build
WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
RUN npm run build

# --- runtime stage -----------------------------------------------------
FROM node:22-slim AS runtime
WORKDIR /app
ENV NODE_ENV=production

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=build /app/dist ./dist
COPY storage ./storage
COPY web ./web
# the compiled Lua script's directory is referenced relative to dist/ratelimiter,
# so copy the .lua file into the dist tree at the same relative path it lives at in src/
COPY src/ratelimiter/tokenbucket.lua ./dist/ratelimiter/tokenbucket.lua

EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/healthz', r => process.exit(r.statusCode===200?0:1)).on('error', () => process.exit(1))"

CMD ["node", "dist/index.js"]
