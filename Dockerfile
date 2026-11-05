# syntax=docker/dockerfile:1

ARG NODE_IMAGE=node:26.12.1-alpine3.24@sha256:40cd011b55415bc894e45b02fdd5a5dfde056921b7d23a64c74f80497af48797
ARG PNPM_VERSION=11.15.1

FROM ${NODE_IMAGE} AS build
ARG PNPM_VERSION

WORKDIR /app
RUN apk upgrade --no-cache && apk add --no-cache bash && npm install --global pnpm@${PNPM_VERSION}

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

FROM ${NODE_IMAGE} AS production
RUN apk upgrade --no-cache \
  && apk add --no-cache bash \
  && rm -rf /usr/local/lib/node_modules /opt/yarn* /usr/local/include/node \
  && rm -f /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack /usr/local/bin/yarn /usr/local/bin/yarnpkg \
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
