# syntax=docker/dockerfile:1

ARG NODE_VERSION=26.5.1
ARG PNPM_VERSION=11.15.1

FROM node:${NODE_VERSION}-alpine AS build

WORKDIR /app
RUN npm install --global pnpm@${PNPM_VERSION}

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml prisma.config.ts ./
COPY prisma ./prisma
RUN pnpm install --frozen-lockfile

COPY . .
RUN pnpm lint \
  && pnpm build \
  && pnpm prune --prod

FROM alpine:3.24 AS production

ARG NODE_VERSION
RUN apk upgrade --no-cache \
  && apk add --no-cache bash nodejs-current=${NODE_VERSION}-r0 \
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
