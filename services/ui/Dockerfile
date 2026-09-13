# ---------------------------------------------------------------------------
# Stage 1: deps — install node_modules once, share across stages
# ---------------------------------------------------------------------------
FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json ./
RUN npm install

# ---------------------------------------------------------------------------
# Stage 2: dev — Vite HMR dev server
#
# node_modules come from the deps stage (baked into the image).
# src/ and index.html are bind-mounted at runtime for live HMR.
# Environment variables from docker-compose control the proxy targets:
#   API_TARGET  — default http://localhost:8080
#   PROM_TARGET — default http://localhost:9090
# ---------------------------------------------------------------------------
FROM node:22-alpine AS dev
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY package.json tsconfig.json tsconfig.node.json \
     vite.config.ts tailwind.config.ts postcss.config.js ./
# src/ and index.html are supplied by the bind mount at runtime.
# Stub copies ensure the image is self-contained if run without mounts.
COPY src ./src
COPY index.html ./
EXPOSE 5173
CMD ["npm", "run", "dev"]

# ---------------------------------------------------------------------------
# Stage 3: build — production bundle (tsc + vite build)
# ---------------------------------------------------------------------------
FROM node:22-alpine AS build
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

# ---------------------------------------------------------------------------
# Stage 4: serve — nginx static file server + reverse proxy
# ---------------------------------------------------------------------------
FROM nginx:1.27-alpine AS serve
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
