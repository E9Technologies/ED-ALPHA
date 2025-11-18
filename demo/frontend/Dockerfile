# Frontend service image for Next.js
FROM node:22-alpine AS deps
WORKDIR /app
RUN corepack enable
COPY demo/frontend/package.json demo/frontend/pnpm-lock.yaml* ./
RUN pnpm install --frozen-lockfile

FROM node:22-alpine AS builder
WORKDIR /app
RUN corepack enable
ARG NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
ENV NEXT_PUBLIC_API_BASE_URL=${NEXT_PUBLIC_API_BASE_URL}
COPY demo/frontend/package.json demo/frontend/pnpm-lock.yaml* ./
RUN pnpm install --frozen-lockfile
COPY demo/frontend/ .
ENV NEXT_TELEMETRY_DISABLED=1
# Some installs land node_modules/.bin scripts without exec bits in certain hosts; fix before build.
RUN find node_modules/.bin -maxdepth 1 -type f -exec chmod +x {} +
RUN pnpm build

FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1
ARG NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
ENV NEXT_PUBLIC_API_BASE_URL=${NEXT_PUBLIC_API_BASE_URL}
RUN corepack enable && corepack prepare pnpm@10.22.0 --activate
COPY --from=builder /app/public ./public
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules
COPY demo/frontend/package.json ./package.json
EXPOSE 3000
USER node
CMD ["pnpm", "start"]
