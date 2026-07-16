import { config } from 'dotenv';
import { defineConfig } from 'prisma/config';

config({ override: false, quiet: true });

/**
 * Adds the configured PostgreSQL schema to the datasource URL used by Prisma
 * CLI commands without exposing the URL in logs.
 *
 * @param databaseUrl - Valid PostgreSQL connection URL.
 * @param schema - Validated PostgreSQL schema name.
 * @returns The datasource URL with its schema query parameter set.
 * @throws {TypeError} When the database URL is invalid.
 */
function withPostgresSchema(databaseUrl: string, schema: string): string {
  const url = new URL(databaseUrl);
  url.searchParams.set('schema', schema);
  return url.toString();
}

const databaseUrl = process.env['DATABASE_URL'];
const postgresSchema = process.env['POSTGRES_SCHEMA'];

export default defineConfig({
  schema: 'prisma/schema.prisma',
  migrations: {
    path: 'prisma/migrations',
  },
  datasource: {
    url:
      databaseUrl && postgresSchema
        ? withPostgresSchema(databaseUrl, postgresSchema)
        : databaseUrl,
  },
});
