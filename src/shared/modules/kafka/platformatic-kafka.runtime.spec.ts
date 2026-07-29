import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { validateEmailServiceEnv } from '../../../config/email-service-env';
import {
  KAFKA_MTLS_CA_CERT,
  KAFKA_MTLS_CLIENT_CERT,
  KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
  KAFKA_MTLS_TEST_PASSPHRASE,
} from '../../../../test/fixtures/kafka-mtls.fixture';

const execFileAsync = promisify(execFile);

/**
 * Creates and validates fixture-backed mTLS options for the runtime boundary.
 *
 * @returns Exact normalized TLS fields accepted by the environment validator.
 * @throws When the shared cryptographic fixture or base environment is invalid.
 */
function createValidatedRuntimeTls(): {
  cert: string;
  key: string;
  ca: string;
  passphrase: string;
} {
  const environment = validateEmailServiceEnv({
    PORT: '3000',
    DATABASE_URL: 'postgresql://user:password@localhost:5432/email',
    POSTGRES_SCHEMA: 'email',
    SENDGRID_API_KEY: 'test-api-key',
    EMAIL_FROM: 'noreply@example.com',
    EMAIL_TEMPLATE_MAP: '{"notifications.email":"d-template-id"}',
    KAFKA_URL: 'localhost:9092',
    KAFKA_CLIENT_ID: 'runtime-compatibility-test',
    KAFKA_GROUP_ID: 'runtime-compatibility-test',
    KAFKA_SSL_ENABLED: 'true',
    KAFKA_CLIENT_CERT: KAFKA_MTLS_CLIENT_CERT,
    KAFKA_CLIENT_CERT_KEY: KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
    KAFKA_CA_CERT: KAFKA_MTLS_CA_CERT,
    KAFKA_CLIENT_CERT_PASSPHRASE: KAFKA_MTLS_TEST_PASSPHRASE,
    KAFKA_CONNECTION_TIMEOUT: '100',
    KAFKA_REQUEST_TIMEOUT: '100',
    KAFKA_RETRY_ATTEMPTS: '1',
    KAFKA_INITIAL_RETRY_TIME: '1',
    KAFKA_MAX_RETRY_TIME: '1',
    KAFKA_MAXBYTES: '1024',
    KAFKA_MAX_WAIT_TIME: '100',
    DISABLE_KAFKA: 'false',
  });

  return {
    cert: environment.KAFKA_CLIENT_CERT!,
    key: environment.KAFKA_CLIENT_CERT_KEY!,
    ca: environment.KAFKA_CA_CERT!,
    passphrase: environment.KAFKA_CLIENT_CERT_PASSPHRASE!,
  };
}

describe('Platformatic Kafka runtime compatibility', () => {
  it('loads the installed ESM package through native import', async () => {
    const script = `
      const kafka = await import('@platformatic/kafka');
      if (typeof kafka.Consumer !== 'function') process.exit(1);
      process.stdout.write('loaded');
    `;

    await expect(
      execFileAsync(process.execPath, [
        '--input-type=module',
        '--eval',
        script,
      ]),
    ).resolves.toEqual(expect.objectContaining({ stdout: 'loaded' }));
  });

  it('accepts validated fixture-backed mTLS without contacting a broker', async () => {
    const tls = createValidatedRuntimeTls();
    const script = `
      const { Consumer } = await import('@platformatic/kafka');
      const tls = ${JSON.stringify(tls)};
      const consumer = new Consumer({
        bootstrapBrokers: ['localhost:9092'],
        clientId: 'runtime-compatibility-test',
        groupId: 'runtime-compatibility-test',
        connectTimeout: 100,
        requestTimeout: 100,
        retries: false,
        tls,
      });
      await consumer.close();
      process.stdout.write('closed');
    `;

    await expect(
      execFileAsync(process.execPath, [
        '--input-type=module',
        '--eval',
        script,
      ]),
    ).resolves.toEqual(expect.objectContaining({ stdout: 'closed' }));
  });
});
