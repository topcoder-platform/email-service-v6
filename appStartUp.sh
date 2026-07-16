#!/bin/bash
set -eo pipefail

if [ -z "${DATABASE_URL:-}" ]; then
    echo "DATABASE_URL is required for database migrations" >&2
    exit 1
fi

if [ -z "${POSTGRES_SCHEMA:-}" ]; then
    echo "POSTGRES_SCHEMA is required for database migrations" >&2
    exit 1
fi

if ! [[ "${POSTGRES_SCHEMA}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "POSTGRES_SCHEMA must be a valid PostgreSQL identifier" >&2
    exit 1
fi

export DATABASE_URL="$(printf '%b' "${DATABASE_URL}")"
if ! normalized_database_url="$(node <<'NODE'
try {
    const url = new URL(process.env.DATABASE_URL);
    url.searchParams.set('schema', process.env.POSTGRES_SCHEMA);
    process.stdout.write(url.toString());
} catch {
    process.stderr.write('database_url_normalization_failed\n');
    process.exitCode = 1;
}
NODE
)"; then
    exit 1
fi
export DATABASE_URL="${normalized_database_url}"
unset normalized_database_url

echo "Using PostgreSQL schema: ${POSTGRES_SCHEMA}"
echo "Bootstrapping PostgreSQL schema"
pnpm run db:migrate

echo "Running database migrations"
pnpm exec prisma migrate deploy

exec node dist/src/main
