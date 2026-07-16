# syntax=docker/dockerfile:1

FROM node:22.18.0-alpine

RUN apk add --no-cache bash
RUN apk update

ENV NODE_ENV=production

WORKDIR /app
COPY . .
RUN npm install pnpm@9.15.9 -g
RUN pnpm install --frozen-lockfile --prod=false
RUN pnpm run lint
RUN pnpm run build
RUN chmod +x appStartUp.sh
EXPOSE 3000
CMD ["./appStartUp.sh"]
