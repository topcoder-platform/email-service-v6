import {
  DEFAULT_EMAIL_RETRY_CRON,
  DEFAULT_EMAIL_RETRY_MAX_AGE_MS,
  KAFKA_MTLS_VALIDATION_ERROR,
  validateEmailServiceEnv,
} from './email-service-env';
import {
  KAFKA_MTLS_CA_CERT,
  KAFKA_MTLS_CLIENT_CERT,
  KAFKA_MTLS_CLIENT_KEY,
  KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
  KAFKA_MTLS_TEST_PASSPHRASE,
  KAFKA_MTLS_UNRELATED_KEY,
  KAFKA_MTLS_WHITESPACE_ENCRYPTED_CLIENT_KEY,
  KAFKA_MTLS_WHITESPACE_TEST_PASSPHRASE,
} from '../../test/fixtures/kafka-mtls.fixture';

/**
 * Creates a complete valid raw environment for focused startup validation tests.
 *
 * @param overrides - Values that replace or remove defaults for a test case.
 * @returns A raw environment object accepted by the config validator.
 */
function createValidEnvironment(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    PORT: '3000',
    DATABASE_URL: 'postgresql://user:password@localhost:5432/email',
    POSTGRES_SCHEMA: 'email',
    SENDGRID_API_KEY: 'test-api-key',
    EMAIL_FROM: 'noreply@example.com',
    EMAIL_TEMPLATE_MAP: '{"notifications.email":"d-template-id"}',
    EMAIL_RETRY_CRON: '0 */5 * * * *',
    EMAIL_RETRY_MAX_AGE_MS: '86400000',
    KAFKA_BROKERS: 'kafka-one:9092,kafka-two:9092',
    KAFKA_CLIENT_ID: 'email-service-v6',
    KAFKA_GROUP_ID: 'email-service-v6',
    KAFKA_SSL_ENABLED: 'false',
    KAFKA_CONNECTION_TIMEOUT: '10000',
    KAFKA_REQUEST_TIMEOUT: '30000',
    KAFKA_RETRY_ATTEMPTS: '5',
    KAFKA_INITIAL_RETRY_TIME: '300',
    KAFKA_MAX_RETRY_TIME: '30000',
    KAFKA_MAX_BYTES: '1048576',
    KAFKA_MAX_WAIT_TIME: '5000',
    DISABLE_KAFKA: 'false',
    ...overrides,
  };
}

/**
 * Asserts that active mTLS validation exposes only the stable safe error.
 *
 * @param overrides - Invalid mTLS values applied to the valid environment.
 * @returns Nothing after asserting the exact error and absence of secret data.
 * @throws Jest assertion failures when unsafe TLS details escape validation.
 */
function expectInvalidMtlsConfiguration(
  overrides: Record<string, unknown>,
): void {
  let thrownError: unknown;
  try {
    validateEmailServiceEnv(
      createValidEnvironment({
        KAFKA_SSL_ENABLED: 'true',
        KAFKA_CLIENT_CERT: KAFKA_MTLS_CLIENT_CERT,
        KAFKA_CLIENT_CERT_KEY: KAFKA_MTLS_CLIENT_KEY,
        ...overrides,
      }),
    );
  } catch (error) {
    thrownError = error;
  }

  expect(thrownError).toBeInstanceOf(Error);
  const error = thrownError as Error;
  expect(error.message).toBe(KAFKA_MTLS_VALIDATION_ERROR);
  expect(Object.prototype.hasOwnProperty.call(error, 'cause')).toBe(false);

  for (const unsafeDetail of [
    '-----BEGIN',
    'test-kafka-passphrase',
    'incorrect-test-passphrase',
    'postgresql://user:password@localhost:5432/email',
    'PEM routines',
    'bad decrypt',
    'no start line',
    'key values mismatch',
    'openssl',
  ]) {
    expect(error.message.toLowerCase()).not.toContain(
      unsafeDetail.toLowerCase(),
    );
  }
}

describe('validateEmailServiceEnv', () => {
  it('parses a valid environment and applies the template override default', () => {
    const environment = validateEmailServiceEnv(createValidEnvironment());

    expect(environment.PORT).toBe(3000);
    expect(environment.KAFKA_BROKERS).toEqual([
      'kafka-one:9092',
      'kafka-two:9092',
    ]);
    expect(environment.EMAIL_TEMPLATE_OVERRIDE_KEY).toBe(
      'sendgrid_template_id',
    );
  });

  it.each([undefined, ''])(
    'applies retry defaults when retry variables are %p',
    (missingValue) => {
      const environment = validateEmailServiceEnv(
        createValidEnvironment({
          EMAIL_RETRY_CRON: missingValue,
          EMAIL_RETRY_MAX_AGE_MS: missingValue,
        }),
      );

      expect(environment.EMAIL_RETRY_CRON).toBe(DEFAULT_EMAIL_RETRY_CRON);
      expect(environment.EMAIL_RETRY_MAX_AGE_MS).toBe(
        DEFAULT_EMAIL_RETRY_MAX_AGE_MS,
      );
    },
  );

  it('supports explicit retry configuration overrides', () => {
    const environment = validateEmailServiceEnv(
      createValidEnvironment({
        EMAIL_RETRY_CRON: '30 */7 * * * *',
        EMAIL_RETRY_MAX_AGE_MS: '7200000',
      }),
    );

    expect(environment.EMAIL_RETRY_CRON).toBe('30 */7 * * * *');
    expect(environment.EMAIL_RETRY_MAX_AGE_MS).toBe(7_200_000);
  });

  it.each(['0', '-1', 'not-a-number'])(
    'fails startup for invalid EMAIL_RETRY_MAX_AGE_MS value %p',
    (value) => {
      expect(() =>
        validateEmailServiceEnv(
          createValidEnvironment({ EMAIL_RETRY_MAX_AGE_MS: value }),
        ),
      ).toThrow('EMAIL_RETRY_MAX_AGE_MS');
    },
  );

  it.each(['DATABASE_URL', 'SENDGRID_API_KEY'])(
    'fails when %s is missing',
    (name) => {
      expect(() =>
        validateEmailServiceEnv(createValidEnvironment({ [name]: undefined })),
      ).toThrow(`${name} is required`);
    },
  );

  it('fails when EMAIL_TEMPLATE_MAP is invalid', () => {
    expect(() =>
      validateEmailServiceEnv(
        createValidEnvironment({ EMAIL_TEMPLATE_MAP: '[]' }),
      ),
    ).toThrow('EMAIL_TEMPLATE_MAP must be a JSON object');
  });

  it('fails when KAFKA_SSL_ENABLED is missing', () => {
    expect(() =>
      validateEmailServiceEnv(
        createValidEnvironment({ KAFKA_SSL_ENABLED: undefined }),
      ),
    ).toThrow('KAFKA_SSL_ENABLED is required');
  });

  it('does not require Kafka client credentials when SSL is disabled', () => {
    expect(() =>
      validateEmailServiceEnv(
        createValidEnvironment({
          KAFKA_SSL_ENABLED: 'false',
          KAFKA_CLIENT_CERT: undefined,
          KAFKA_CLIENT_CERT_KEY: undefined,
        }),
      ),
    ).not.toThrow();
  });

  it.each(['KAFKA_CLIENT_CERT', 'KAFKA_CLIENT_CERT_KEY'])(
    'fails when SSL-enabled Kafka is missing %s',
    (name) => {
      expectInvalidMtlsConfiguration({ [name]: undefined });
    },
  );

  it('accepts a matching certificate and private key without an optional CA', () => {
    const environment = validateEmailServiceEnv(
      createValidEnvironment({
        KAFKA_SSL_ENABLED: 'true',
        KAFKA_CLIENT_CERT: KAFKA_MTLS_CLIENT_CERT,
        KAFKA_CLIENT_CERT_KEY: KAFKA_MTLS_CLIENT_KEY,
        KAFKA_CA_CERT: undefined,
      }),
    );

    expect(environment.KAFKA_CLIENT_CERT).toBe(KAFKA_MTLS_CLIENT_CERT);
    expect(environment.KAFKA_CLIENT_CERT_KEY).toBe(KAFKA_MTLS_CLIENT_KEY);
    expect(environment.KAFKA_CA_CERT).toBeUndefined();
  });

  it('accepts an encrypted matching key with the correct passphrase and CA', () => {
    const environment = validateEmailServiceEnv(
      createValidEnvironment({
        KAFKA_SSL_ENABLED: 'true',
        KAFKA_CLIENT_CERT: KAFKA_MTLS_CLIENT_CERT,
        KAFKA_CLIENT_CERT_KEY: KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
        KAFKA_CA_CERT: KAFKA_MTLS_CA_CERT,
        KAFKA_CLIENT_CERT_PASSPHRASE: KAFKA_MTLS_TEST_PASSPHRASE,
      }),
    );

    expect(environment.KAFKA_CLIENT_CERT_KEY).toBe(
      KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
    );
    expect(environment.KAFKA_CLIENT_CERT_PASSPHRASE).toBe(
      KAFKA_MTLS_TEST_PASSPHRASE,
    );
    expect(environment.KAFKA_CA_CERT).toBe(KAFKA_MTLS_CA_CERT);
  });

  it('preserves a whitespace-sensitive private-key passphrase exactly', () => {
    const environment = validateEmailServiceEnv(
      createValidEnvironment({
        KAFKA_SSL_ENABLED: 'true',
        KAFKA_CLIENT_CERT: KAFKA_MTLS_CLIENT_CERT,
        KAFKA_CLIENT_CERT_KEY: KAFKA_MTLS_WHITESPACE_ENCRYPTED_CLIENT_KEY,
        KAFKA_CLIENT_CERT_PASSPHRASE: KAFKA_MTLS_WHITESPACE_TEST_PASSPHRASE,
      }),
    );

    expect(environment.KAFKA_CLIENT_CERT_PASSPHRASE).toBe(
      KAFKA_MTLS_WHITESPACE_TEST_PASSPHRASE,
    );
  });

  it('treats an exactly empty private-key passphrase as omitted', () => {
    const environment = validateEmailServiceEnv(
      createValidEnvironment({
        KAFKA_SSL_ENABLED: 'true',
        KAFKA_CLIENT_CERT: KAFKA_MTLS_CLIENT_CERT,
        KAFKA_CLIENT_CERT_KEY: KAFKA_MTLS_CLIENT_KEY,
        KAFKA_CLIENT_CERT_PASSPHRASE: '',
      }),
    );

    expect(environment.KAFKA_CLIENT_CERT_PASSPHRASE).toBeUndefined();
  });

  it('accepts a valid multi-certificate CA bundle', () => {
    const caBundle = `${KAFKA_MTLS_CA_CERT}\n${KAFKA_MTLS_CLIENT_CERT}`;
    const environment = validateEmailServiceEnv(
      createValidEnvironment({
        KAFKA_SSL_ENABLED: 'true',
        KAFKA_CLIENT_CERT: KAFKA_MTLS_CLIENT_CERT,
        KAFKA_CLIENT_CERT_KEY: KAFKA_MTLS_CLIENT_KEY,
        KAFKA_CA_CERT: caBundle,
      }),
    );

    expect(environment.KAFKA_CA_CERT).toBe(caBundle);
  });

  it('normalizes escaped newlines before cryptographic validation', () => {
    const escapedCertificate = KAFKA_MTLS_CLIENT_CERT.replace(/\n/g, '\\n');
    const escapedKey = KAFKA_MTLS_CLIENT_KEY.replace(/\n/g, '\\n');
    const escapedCa = KAFKA_MTLS_CA_CERT.replace(/\n/g, '\\n');
    const environment = validateEmailServiceEnv(
      createValidEnvironment({
        KAFKA_SSL_ENABLED: 'true',
        KAFKA_CLIENT_CERT: escapedCertificate,
        KAFKA_CLIENT_CERT_KEY: escapedKey,
        KAFKA_CA_CERT: escapedCa,
      }),
    );

    expect(environment.KAFKA_CLIENT_CERT).toBe(KAFKA_MTLS_CLIENT_CERT);
    expect(environment.KAFKA_CLIENT_CERT_KEY).toBe(KAFKA_MTLS_CLIENT_KEY);
    expect(environment.KAFKA_CA_CERT).toBe(KAFKA_MTLS_CA_CERT);
  });

  it('rejects a malformed client certificate with only the stable error', () => {
    expectInvalidMtlsConfiguration({
      KAFKA_CLIENT_CERT:
        '-----BEGIN CERTIFICATE-----\nmalformed-test-content\n-----END CERTIFICATE-----',
    });
  });

  it.each([
    '-----BEGIN PRIVATE KEY-----\nmalformed-test-content\n-----END PRIVATE KEY-----',
    KAFKA_MTLS_CLIENT_CERT,
  ])(
    'rejects a malformed or unusable private key with only the stable error',
    (key) => {
      expectInvalidMtlsConfiguration({ KAFKA_CLIENT_CERT_KEY: key });
    },
  );

  it('rejects a malformed optional CA with only the stable error', () => {
    expectInvalidMtlsConfiguration({
      KAFKA_CA_CERT:
        '-----BEGIN CERTIFICATE-----\nmalformed-test-ca\n-----END CERTIFICATE-----',
    });
  });

  it('rejects a valid CA followed by a malformed certificate with only the stable error', () => {
    expectInvalidMtlsConfiguration({
      KAFKA_CA_CERT: `${KAFKA_MTLS_CA_CERT}\n-----BEGIN CERTIFICATE-----\nmalformed-trailing-ca\n-----END CERTIFICATE-----`,
    });
  });

  it.each([
    ['unexpected content', `${KAFKA_MTLS_CA_CERT}\nunexpected-content`],
    [
      'an incomplete delimiter',
      `${KAFKA_MTLS_CA_CERT}\n-----BEGIN CERTIFICATE-----\nincomplete-ca`,
    ],
    ['an empty bundle', ' \n\t '],
  ])(
    'rejects a CA bundle containing %s with only the stable error',
    (_, ca) => {
      expectInvalidMtlsConfiguration({ KAFKA_CA_CERT: ca });
    },
  );

  it.each([undefined, 'incorrect-test-passphrase'])(
    'rejects an encrypted key with unusable passphrase %p',
    (passphrase) => {
      expectInvalidMtlsConfiguration({
        KAFKA_CLIENT_CERT_KEY: KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
        KAFKA_CLIENT_CERT_PASSPHRASE: passphrase,
      });
    },
  );

  it('rejects incompatible certificate and private-key pairs', () => {
    expectInvalidMtlsConfiguration({
      KAFKA_CLIENT_CERT_KEY: KAFKA_MTLS_UNRELATED_KEY,
    });
  });

  it('bypasses malformed mTLS material when Kafka is disabled', () => {
    expect(() =>
      validateEmailServiceEnv(
        createValidEnvironment({
          DISABLE_KAFKA: 'true',
          KAFKA_SSL_ENABLED: 'true',
          KAFKA_CLIENT_CERT: 'malformed-client-certificate',
          KAFKA_CLIENT_CERT_KEY: 'malformed-client-key',
          KAFKA_CA_CERT: 'malformed-ca-certificate',
          KAFKA_CLIENT_CERT_PASSPHRASE: 'unused-passphrase',
        }),
      ),
    ).not.toThrow();
  });

  it('accepts plain Kafka without cryptographic validation', () => {
    expect(() =>
      validateEmailServiceEnv(
        createValidEnvironment({
          KAFKA_SSL_ENABLED: 'false',
          KAFKA_CLIENT_CERT: 'malformed-client-certificate',
          KAFKA_CLIENT_CERT_KEY: 'malformed-client-key',
          KAFKA_CA_CERT: 'malformed-ca-certificate',
          KAFKA_CLIENT_CERT_PASSPHRASE: 'unused-passphrase',
        }),
      ),
    ).not.toThrow();
  });
});
