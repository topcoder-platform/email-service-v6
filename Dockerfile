# syntax=docker/dockerfile:1

ARG NODE_IMAGE=node:26.13.1-alpine3.24@sha256:6bd9ab43c0ec0190ca7b2d193906cb8b7edf6e7f4afa43ddeccd6452710bffb5
ARG PNPM_VERSION=11.15.1

FROM ${NODE_IMAGE} AS build
ARG PNPM_VERSION

WORKDIR /app
RUN apk upgrade --no-cache && apk add --no-cache 'zlib>=1.4-r0' bash && npm install --global pnpm@${PNPM_VERSION}

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml prisma.config.ts ./
COPY prisma ./prisma
COPY tools ./tools
RUN pnpm install --frozen-lockfile

COPY . .
RUN pnpm lint \
  && pnpm test --runInBand \
  && pnpm test:dependency-compat \
  && pnpm build \
  && pnpm prune --prod

FROM alpine:3.24 AS production
RUN apk upgrade --no-cache \
    && apk add --no-cache ca-certificates 'nodejs-current~26.13.1' bash 'zlib>=1.4-r0' 'libcrypto3>=3.5.9-r1' 'libssl3>=3.5.9-r1' \
    && addgroup -S app \
    && adduser -S -D -u 10001 -G app app

ENV NODE_ENV=production
WORKDIR /app

COPY --from=build --chown=app:app /app/dist ./dist
COPY --from=build --chown=app:app /app/node_modules ./node_modules
COPY --from=build --chown=app:app /app/package.json ./package.json
COPY --from=build --chown=app:app /app/prisma ./prisma
COPY --from=build --chown=app:app /app/prisma.config.ts ./prisma.config.ts
COPY --from=build --chown=app:app --chmod=0555 /app/appStartUp.sh ./appStartUp.sh

USER app
EXPOSE 6100

CMD ["./appStartUp.sh"]
