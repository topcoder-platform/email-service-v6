import type { EmailServiceConfigService } from '../../../config/email-service-config.service';
import {
  KAFKA_MTLS_CA_CERT,
  KAFKA_MTLS_CLIENT_CERT,
  KAFKA_MTLS_CLIENT_KEY,
  KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
  KAFKA_MTLS_TEST_PASSPHRASE,
} from '../../../../test/fixtures/kafka-mtls.fixture';
import {
  PlatformaticKafkaClientFactory,
  type PlatformaticKafkaModuleLoader,
} from './platformatic-kafka-client.factory';

/**
 * Creates config access with focused Kafka overrides.
 *
 * @param overrides - Kafka values that replace the valid test defaults.
 * @returns A config-service double accepted by the factory.
 */
function createConfig(
  overrides: Record<string, unknown> = {},
): EmailServiceConfigService {
  return {
    kafka: {
      brokers: ['kafka-one:9092', 'kafka-two:9092'],
      clientId: 'email-service-v6',
      groupId: 'email-workers',
      sslEnabled: true,
      clientCert: KAFKA_MTLS_CLIENT_CERT,
      clientCertKey: KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
      caCert: KAFKA_MTLS_CA_CERT,
      clientCertPassphrase: KAFKA_MTLS_TEST_PASSPHRASE,
      connectionTimeout: 12_345,
      requestTimeout: 54_321,
      retryAttempts: 5,
      initialRetryTime: 100,
      maxRetryTime: 1000,
      maxBytes: 1024,
      maxWaitTime: 500,
      disabled: false,
      ...overrides,
    },
  } as EmailServiceConfigService;
}

describe('PlatformaticKafkaClientFactory', () => {
  const constructedOptions: unknown[] = [];
  const consumer = { consume: jest.fn(), close: jest.fn() };
  const Consumer = jest.fn((options: unknown) => {
    constructedOptions.push(options);
    return consumer;
  });
  const loader = jest.fn(() =>
    Promise.resolve({
      Consumer,
    }),
  ) as unknown as jest.MockedFunction<PlatformaticKafkaModuleLoader>;

  beforeEach(() => {
    jest.clearAllMocks();
    constructedOptions.length = 0;
  });

  it('maps exact cert, key, CA, and passphrase TLS fields', async () => {
    const factory = new PlatformaticKafkaClientFactory(createConfig(), loader);

    await expect(factory.createConsumer()).resolves.toBe(consumer);

    expect(constructedOptions[0]).toEqual({
      bootstrapBrokers: ['kafka-one:9092', 'kafka-two:9092'],
      clientId: 'email-service-v6',
      groupId: 'email-workers',
      connectTimeout: 12_345,
      requestTimeout: 54_321,
      retries: false,
      tls: {
        cert: KAFKA_MTLS_CLIENT_CERT,
        key: KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
        ca: KAFKA_MTLS_CA_CERT,
        passphrase: KAFKA_MTLS_TEST_PASSPHRASE,
      },
    });
  });

  it('omits optional TLS fields when they are not configured', async () => {
    const factory = new PlatformaticKafkaClientFactory(
      createConfig({
        clientCertKey: KAFKA_MTLS_CLIENT_KEY,
        caCert: undefined,
        clientCertPassphrase: undefined,
      }),
      loader,
    );

    await factory.createConsumer();

    expect(constructedOptions[0]).toEqual(
      expect.objectContaining({
        tls: {
          cert: KAFKA_MTLS_CLIENT_CERT,
          key: KAFKA_MTLS_CLIENT_KEY,
        },
      }),
    );
  });

  it('configures connection and request timeouts with no internal retries', async () => {
    const factory = new PlatformaticKafkaClientFactory(
      createConfig({ sslEnabled: false }),
      loader,
    );

    await factory.createConsumer();

    expect(constructedOptions[0]).toEqual({
      bootstrapBrokers: ['kafka-one:9092', 'kafka-two:9092'],
      clientId: 'email-service-v6',
      groupId: 'email-workers',
      connectTimeout: 12_345,
      requestTimeout: 54_321,
      retries: false,
    });
  });

  it('does not load the package or downgrade when TLS material is missing', async () => {
    const factory = new PlatformaticKafkaClientFactory(
      createConfig({ clientCertKey: undefined }),
      loader,
    );

    await expect(factory.createConsumer()).rejects.toThrow(
      'Kafka mTLS certificate and key are required',
    );
    expect(loader).not.toHaveBeenCalled();
    expect(Consumer).not.toHaveBeenCalled();
  });

  it('propagates ESM package-loading failures without creating a fallback', async () => {
    loader.mockRejectedValueOnce(new Error('ESM package unavailable'));
    const factory = new PlatformaticKafkaClientFactory(createConfig(), loader);

    await expect(factory.createConsumer()).rejects.toThrow(
      'ESM package unavailable',
    );
    expect(Consumer).not.toHaveBeenCalled();
  });
});
