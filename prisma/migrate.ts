import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '@prisma/client';
import { config } from 'dotenv';

/**
 * Checks for the configured PostgreSQL schema and creates it only when absent.
 *
 * This bootstrap runs before `prisma migrate deploy` so Prisma can create its
 * migration table and application objects in a fresh, non-public schema.
 *
 * Environment values are loaded from `.env` without replacing variables that
 * were already exported by the deployment environment. All validation and
 * client setup occurs inside this asynchronous boundary so its caller can emit
 * one credential-free failure event for every bootstrap failure.
 *
 * Existing schemas are reused without requiring database-level schema-creation
 * permission. If another bootstrap creates an initially absent schema first,
 * the losing attempt rechecks and accepts the completed state.
 *
 * @returns A promise that resolves after the schema exists and Prisma disconnects.
 * @throws Rethrows environment, client setup, connection, schema creation, or
 * disconnect errors so the top-level boundary can stop startup safely.
 */
async function bootstrapSchema(): Promise<void> {
  config({ override: false, quiet: true });

  const databaseUrl = process.env['DATABASE_URL'];
  if (!databaseUrl) {
    throw new Error(
      'DATABASE_URL must be set to bootstrap the database schema.',
    );
  }

  const postgresSchema = process.env['POSTGRES_SCHEMA'];
  if (!postgresSchema) {
    throw new Error(
      'POSTGRES_SCHEMA must be set to bootstrap the database schema.',
    );
  }

  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(postgresSchema)) {
    throw new Error('POSTGRES_SCHEMA must be a valid PostgreSQL identifier.');
  }

  const adapter = new PrismaPg(
    { connectionString: databaseUrl },
    { schema: postgresSchema },
  );
  const prisma = new PrismaClient({ adapter });

  try {
    const [schema] = await prisma.$queryRaw<Array<{ exists: boolean }>>`
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.schemata
        WHERE schema_name = ${postgresSchema}
      ) AS "exists"
    `;

    if (!schema?.exists) {
      try {
        await prisma.$executeRawUnsafe(`CREATE SCHEMA "${postgresSchema}"`);
      } catch (error) {
        const [createdConcurrently] = await prisma.$queryRaw<
          Array<{ exists: boolean }>
        >`
          SELECT EXISTS (
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = ${postgresSchema}
          ) AS "exists"
        `;
        if (!createdConcurrently?.exists) {
          throw error;
        }
      }
    }
    console.log('database_schema_bootstrap_succeeded');
  } finally {
    await prisma.$disconnect();
  }
}

void bootstrapSchema().catch(() => {
  console.error('database_schema_bootstrap_failed');
  process.exitCode = 1;
});
