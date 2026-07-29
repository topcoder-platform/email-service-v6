# Email Service v6

Email Service v6 is a NestJS worker and operational HTTP API. It consumes email
requests from Kafka, normalizes either legacy or v3 payloads, records each
delivery attempt in PostgreSQL through Prisma, and sends through SendGrid. A
process-local scheduler retries recent failed attempts. The HTTP surface is
limited to dependency-aware health reporting.

## Runtime flow

For an accepted Kafka message, the service:

1. Decodes a raw email payload or a bus envelope containing `payload`.
2. Validates recipients and supported SendGrid fields.
3. Resolves a SendGrid template from a payload override or the topic map.
4. stores a normalized `PENDING` attempt in PostgreSQL.
5. Sends the normalized payload through SendGrid.
6. Marks the attempt `SUCCESS` or `FAILED` with its attempt timestamp.

Kafka broker connection happens without blocking HTTP startup. Configuration,
Prisma initialization, and retry-schedule registration are startup-critical and
fail fast. When active Kafka SSL is configured, Node validates the normalized
client certificate, private key, optional CA, and optional passphrase locally
before Kafka starts. This performs no broker connection. Invalid or incompatible
material stops startup with the stable safe error
`Invalid Kafka mTLS configuration`; underlying TLS details are not exposed.
Prisma is initialized even when Kafka is disabled.

## Prerequisites and local setup

- Node.js 22.23.1, selected from `.nvmrc`
- pnpm 9.15.9
- PostgreSQL reachable through `DATABASE_URL`
- Kafka unless `DISABLE_KAFKA=true`
- SendGrid credentials

```bash
nvm use
corepack enable
corepack prepare pnpm@9.15.9 --activate
pnpm install --frozen-lockfile
cp .env.sample .env
pnpm run db:migrate
pnpm exec prisma migrate deploy
pnpm start:dev
```

Both migration commands load `.env` directly, while variables already exported
by a deployment environment retain precedence. The configured schema is added
to the Prisma connection URL automatically for bootstrap and CLI migrations.

Local environment variants, host dependencies, generated outputs, and local
private-key or certificate files are excluded from both Git and Docker build
contexts. `.env.sample` is intentionally retained as the configuration template
and must contain placeholders only, never real credentials or secret material.

## Environment contract

Never commit real API keys, passwords, certificates, private keys, or
passphrases. Supply secrets through the deployment platform's secret manager.
PEM values may use escaped `\n` newlines. A CA bundle may contain multiple PEM
certificates, but every block must be complete and valid, with only whitespace
between blocks. Logs deliberately use stable event names and do not include
payloads, recipients, provider errors, credentials, or dependency failure
details.

| Variable | Required | Default / behavior |
| --- | --- | --- |
| `PORT` | No | `3000` |
| `DATABASE_URL` | Yes | PostgreSQL connection URL |
| `POSTGRES_SCHEMA` | Yes | Valid PostgreSQL identifier used by Prisma and startup bootstrap |
| `SENDGRID_API_KEY` | Yes | No default; inject as a secret |
| `EMAIL_FROM` | Yes | Default sender and reply-to address for omitted or empty values |
| `EMAIL_TEMPLATE_MAP` | Yes | Non-empty JSON object mapping Kafka topics to SendGrid template IDs |
| `EMAIL_TEMPLATE_OVERRIDE_KEY` | No | `sendgrid_template_id` |
| `EMAIL_RETRY_CRON` | No | `0 */2 * * * *`; six-field cron including seconds |
| `EMAIL_RETRY_MAX_AGE_MS` | No | `86400000` (24 hours); explicit values must be positive integers |
| `DISABLE_KAFKA` | No | `false`; `true` disables consumer startup and reports Kafka as `disabled` |
| `KAFKA_URL` | Unless Kafka is disabled | Shared comma-separated, scheme-less `host:port` broker addresses from `/config/common/global-appvar` |
| `KAFKA_CLIENT_ID` | Unless Kafka is disabled | Kafka client identity |
| `KAFKA_GROUP_ID` | Unless Kafka is disabled | Consumer group identity |
| `KAFKA_SSL_ENABLED` | Yes | Boolean; `true` enables local mTLS validation when Kafka is active |
| `KAFKA_CLIENT_CERT` | When Kafka is active and SSL is enabled | Client certificate; secret value |
| `KAFKA_CLIENT_CERT_KEY` | When Kafka is active and SSL is enabled | Client private key; secret value |
| `KAFKA_CA_CERT` | No | Optional CA certificate bundle; a supplied empty bundle is invalid when mTLS validation is active |
| `KAFKA_CLIENT_CERT_PASSPHRASE` | No | Optional private-key passphrase; non-empty values preserve exact whitespace, while an empty string is treated as omitted; secret value |
| `KAFKA_CONNECTION_TIMEOUT` | Yes | Positive integer milliseconds |
| `KAFKA_REQUEST_TIMEOUT` | Yes | Positive integer milliseconds |
| `KAFKA_BROKER_TIMEOUT` | No | `5000`; positive integer broker-operation timeout, mapped independently from the client request deadline |
| `KAFKA_SESSION_TIMEOUT` | No | `60000`; positive integer consumer-group session timeout |
| `KAFKA_HEARTBEAT_INTERVAL` | No | `3000`; positive integer consumer-group heartbeat interval |
| `KAFKA_RETRY_ATTEMPTS` | Yes | Positive integer reconnect budget |
| `KAFKA_INITIAL_RETRY_TIME` | Yes | Positive integer initial backoff milliseconds |
| `KAFKA_MAX_RETRY_TIME` | Yes | Positive integer maximum backoff milliseconds |
| `KAFKA_MAXBYTES` | Yes | Shared positive integer consumer fetch limit from `/config/common/global-appvar` |
| `KAFKA_MAX_WAIT_TIME` | Yes | Positive integer consumer wait milliseconds |

Kafka timing must also satisfy `KAFKA_MAX_WAIT_TIME <
KAFKA_REQUEST_TIMEOUT`, `KAFKA_BROKER_TIMEOUT < KAFKA_REQUEST_TIMEOUT`, and
`KAFKA_HEARTBEAT_INTERVAL + KAFKA_REQUEST_TIMEOUT <
KAFKA_SESSION_TIMEOUT`. These relationships keep broker and fetch waits within
the client deadline while leaving enough session time for a delayed heartbeat.

When `DISABLE_KAFKA=true`, brokers, client ID, group ID, client certificate, and
client key are not required. The remaining Kafka booleans and numeric settings
are still validated as part of the single v6 configuration contract.

Cryptographic validation is bypassed when `DISABLE_KAFKA=true` or
`KAFKA_SSL_ENABLED=false`. With active mTLS, malformed certs, keys, CAs,
unusable passphrases, and cert/key mismatches are invalid configuration and fail
before Kafka startup. A valid mTLS configuration with an unreachable broker is
a runtime availability condition instead: the HTTP listener still starts while
Kafka enters its bounded reconnect flow.

## Kafka client and recovery

The consumer uses exactly `@platformatic/kafka` 2.8.0. It subscribes in
committed-offset mode, preserves `latest` as the first-run fallback, and passes
the deployment-controlled `KAFKA_MAXBYTES` value on every subscription.
Platformatic 2.x increased its implicit consumer default to 50 MiB, but this
service never uses that implicit value. The shared setting (1 MiB in
`.env.sample`) remains authoritative, so a hard-coded 10 MiB compatibility cap
is neither needed nor introduced.

Consumer-client errors, message-stream errors or termination, and failed
autocommit operations all enter the same bounded external reconnect lifecycle.
Each attempt creates a fresh consumer, uses equal-jitter exponential backoff,
and cannot report `ready` if the replacement client fails while startup is
settling. Shutdown invalidates current generations, cancels pending backoff,
waits for in-flight email processing, and contains late errors from resources
whose asynchronous close failed.

## Message compatibility and SendGrid mapping

The Kafka value may be a raw JSON email object:

```json
{
  "recipients": [{ "email": "member@example.com", "name": "Member" }],
  "data": { "handle": "member" }
}
```

It may also use the bus-envelope form:

```json
{
  "payload": {
    "recipients": ["member@example.com"],
    "data": { "handle": "member" }
  }
}
```

`recipients` must be a non-empty array of email strings or `{ email, name? }`
objects. Supported optional fields are `from`, `replyTo`, `data`, `categories`,
`cc`, `bcc`, `version`, `attachments`, `personalizations`, and `sendAt`.
Attachments and personalizations are structurally validated before delivery.

The configured override field (by default `sendgrid_template_id`) takes
precedence over `EMAIL_TEMPLATE_MAP`. An invalid supplied override causes the
message to be skipped; it does not fall back to the topic map.

- For legacy payloads, `data` maps to SendGrid `substitutions` with `{{` and
  `}}` substitution wrappers.
- When `version` is exactly `v3`, `data` maps to `dynamicTemplateData` and the
  service also maps attachments, personalizations, and optional `sendAt`.
- Both mappings preserve categories, cc, and bcc. Missing or empty `from` and
  `replyTo` values use `EMAIL_FROM`.

## Retry behavior

The default six-field schedule runs at second zero every two minutes. Each cycle
selects only `FAILED` attempts whose `createdAt` is within the configured
24-hour window, ordered oldest first. It never selects `PENDING` attempts.

Retries reuse the exact normalized payload and resolved template ID stored for
the initial attempt and call the same SendGrid delivery service. Every accepted
or rejected retry increments `retryCount` and records one `lastAttemptedAt`
timestamp. Accepted retries become `SUCCESS` and set `sentAt`; rejected retries
remain `FAILED` with a stable sanitized failure description.

An in-memory guard prevents overlapping retry cycles within one process and is
always released after success or contained failure. This is not distributed
coordination. `PENDING` attempts left by a process crash are never retried and
may require manual operational cleanup.

Deploy with an ECS desired count of exactly one and retain stop-before-start
rolling settings (`minimumHealthyPercent=0`, `maximumPercent=100`). Horizontal
scaling or overlapping old and new tasks requires distributed retry coordination
or a single dedicated retry worker; otherwise multiple processes can select and
send the same failed attempt.

## Health endpoint

Non-production routing is `GET /email/healthcheck`. In production, `main.ts`
applies the `/v6` global prefix, producing `GET /v6/email/healthcheck`.

Every request performs a fresh `SELECT 1` Prisma query and reads an immutable
Kafka status snapshot. The response body is stable and does not expose database
exceptions, Kafka failure reasons, broker addresses, credentials, certificates,
payloads, or stack traces:

```json
{
  "status": "healthy",
  "database": { "status": "connected" },
  "kafka": { "state": "ready", "reconnectAttempts": 0 }
}
```

With a connected database, Kafka states `initializing`, `ready`,
`reconnecting`, and `disabled` return HTTP 200. `initializing` is deliberately
healthy because startup recovery is still internal and ECS should not restart
the task prematurely. Kafka `failed` returns HTTP 503 because its recovery
budget is exhausted. An unavailable database returns HTTP 503 for every Kafka
state. Unhealthy responses use the same schema with `status: "unhealthy"` and
`database.status: "unavailable"` where applicable.

## Container startup and deployment

The multi-stage image uses Node 22.23.1, builds with pnpm 9.15.9, retains only
production dependencies, runs as the unprivileged `node` user, and exposes the
development service port 6100. The executable `appStartUp.sh` uses fail-fast
shell semantics and performs startup in this order:

1. Validate required database settings and normalize the connection URL.
2. Bootstrap the configured PostgreSQL schema.
3. Run `prisma migrate deploy`.
4. Execute the compiled application.

Any schema bootstrap or migration failure stops application startup. Production
sets `NODE_ENV=production`, so the application adds the `/v6` route prefix. An
existing configured schema requires permission to use and migrate objects in
that schema; database-level schema-creation permission is needed only when the
schema is absent. Concurrent first-time starters safely accept a schema created
by the other process after rechecking the final database state.

The development ECS configuration retains the legacy service's port 6100, so
the Parameter Store `PORT`, image metadata, and ECS container/target-group port
remain aligned. The application default outside that deployment remains 3000.
The `develop`
CircleCI workflow builds the image, reads `/config/email-service-v6/deployvar`,
registers a new task-definition revision, and updates the existing
`email-service-v6` Fargate service. Runtime configuration comes from
`/config/email-service-v6/appvar` and `/config/common/global-appvar`.

CircleCI intentionally does not run CloudFormation or create the ECS cluster,
service, or load-balancer routing. The one-time shared bootstrap is maintained
in the sibling `topcoder-infrastructure-cloudformation` directory. The ECS
desired count must remain exactly one and deployments must remain
stop-before-start because retry scheduling is process-local.

## Migration from earlier services

- Rename `TEMPLATE_MAP` to `EMAIL_TEMPLATE_MAP`.
- Consume the shared `KAFKA_URL` and `KAFKA_MAXBYTES` variables directly; do
  not duplicate them under the service-specific application path.
- Remove `API_CONTEXT_PATH`; v6 owns its production prefix.

There is no fallback to legacy environment names and no migration of Sequelize
email-attempt records. Existing old records remain outside the v6 Prisma model.

## Non-goals

This release does not provide a Kafka dead-letter queue, a public email
management API, distributed retry locking, legacy environment compatibility,
or migration of old delivery records.

## Validation

Run the project checks using the pinned Node version:

```bash
nvm use
pnpm lint
pnpm build
pnpm test -- --runInBand repository-ignore-policy.spec.ts
pnpm test -- --runInBand
```
