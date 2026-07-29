# syntax=docker/dockerfile:1

FROM node:22.23.1-alpine AS build

WORKDIR /app
RUN npm install --global pnpm@9.15.9

COPY package.json pnpm-lock.yaml prisma.config.ts ./
COPY prisma ./prisma
RUN pnpm install --frozen-lockfile

COPY . .
RUN pnpm lint \
  && pnpm build \
  && pnpm prune --prod

FROM node:22.23.1-alpine AS production

RUN apk add --no-cache bash

ENV NODE_ENV=production
WORKDIR /app

COPY --from=build --chown=node:node /app/dist ./dist
COPY --from=build --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/package.json ./package.json
COPY --from=build --chown=node:node /app/prisma ./prisma
COPY --from=build --chown=node:node /app/prisma.config.ts ./prisma.config.ts
COPY --from=build --chown=node:node --chmod=0555 /app/appStartUp.sh ./appStartUp.sh

USER node
EXPOSE 6100

CMD ["./appStartUp.sh"]
