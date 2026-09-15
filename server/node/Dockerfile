FROM node:26-alpine

# Declare unused BuildKit build arguments passed by docker-compose to silence warnings
ARG EXAMPLE=""
ARG FEATURES=""

WORKDIR /app

# 1. Copy package definitions first for layer caching
COPY server/node/package*.json ./server/node/

WORKDIR /app/server/node
# Install dependencies including ioredis (required for Redis clustering)
RUN npm install

WORKDIR /app

# 2. Copy monorepo assets required for the web server, client, and tests
COPY client/ ./client/
COPY examples/ ./examples/
COPY tests/ ./tests/
COPY server/node/ ./server/node/

WORKDIR /app/server/node

ENV NODE_ENV=production
ENV PORT=8080

EXPOSE 8080 8081

CMD ["node", "examples/main.js"]
