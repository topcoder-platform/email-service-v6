import { EventEmitter } from 'node:events';
import type { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { validateEmailServiceEnv } from '../../../config/email-service-env';
import {
  KAFKA_MTLS_CA_CERT,
  KAFKA_MTLS_CLIENT_CERT,
  KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
  KAFKA_MTLS_TEST_PASSPHRASE,
} from '../../../../test/fixtures/kafka-mtls.fixture';
import { LoggerService } from '../global/logger.service';
import { KafkaConsumerService } from './kafka-consumer.service';
import {
  KafkaConnectionState,
  type KafkaConsumerClient,
  type KafkaMessage,
  type KafkaMessageStream,
} from './kafka.types';

/** Controllable async stream used to exercise lifecycle behavior deterministically. */
class TestMessageStream extends EventEmitter implements KafkaMessageStream {
  private readonly messages: KafkaMessage[] = [];
  private waiting?: () => void;
  private closed = false;
  private closeFailure?: Error;
  readonly close = jest.fn((): Promise<void> => {
    this.closed = true;
    this.waiting?.();
    return this.closeFailure === undefined
      ? Promise.resolve()
      : Promise.reject(this.closeFailure);
  });

  /**
   * Makes close end iteration normally but reject its resource-close promise.
   *
   * @param error - Error returned by the next and subsequent close attempts.
   * @returns Nothing.
   * @throws Never; the failure is retained until the close mock is invoked.
   */
  failClose(error: Error): void {
    this.closeFailure = error;
  }

  /**
   * Adds one message and wakes the active async iterator.
   *
   * @param message - Message to expose to the consumer service.
   * @returns Nothing.
   * @throws Never; the in-memory queue accepts typed messages only.
   */
  pushMessage(message: KafkaMessage): void {
    this.messages.push(message);
    this.waiting?.();
    this.waiting = undefined;
  }

  /**
   * Yields queued messages and waits until another arrives or close is called.
   *
   * @returns An async iterator over controllable Kafka test messages.
   * @throws Never; test-controlled closure ends iteration normally.
   */
  async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    while (!this.closed) {
      const message = this.messages.shift();
      if (message !== undefined) {
        yield message;
        continue;
      }
      await new Promise<void>((resolve) => {
        this.waiting = resolve;
      });
    }
  }
}

/** Consumer test double with observable consume and close operations. */
class TestConsumer extends EventEmitter implements KafkaConsumerClient {
  readonly consume = jest.fn();
  readonly close = jest.fn((): Promise<void> => Promise.resolve());

  /**
   * Creates a consumer that resolves subscription with the supplied stream.
   *
   * @param stream - Stream returned by consume.
   * @throws Never; the constructor only sets the test mock implementation.
   */
  constructor(stream: KafkaMessageStream) {
    super();
    this.consume.mockResolvedValue(stream);
  }
}

/**
 * Creates complete config access for consumer lifecycle tests.
 *
 * @param overrides - Kafka settings that replace deterministic defaults.
 * @returns A typed config-service double.
 */
function createConfig(
  overrides: Record<string, unknown> = {},
): EmailServiceConfigService {
  return {
    email: {
      templateMap: {
        'notifications.email': 'd-one',
        'billing.email': 'd-two',
      },
    },
    kafka: {
      disabled: false,
      retryAttempts: 2,
      initialRetryTime: 10,
      maxRetryTime: 15,
      maxBytes: 2048,
      maxWaitTime: 250,
      clientCert: 'CERTIFICATE-CONTENT',
      clientCertKey: 'PRIVATE-KEY-CONTENT',
      caCert: 'CA-CONTENT',
      clientCertPassphrase: 'PASSPHRASE-CONTENT',
      ...overrides,
    },
  } as EmailServiceConfigService;
}

/**
 * Creates consumer config whose full mTLS shape passed environment validation.
 *
 * @returns A typed fixture-backed Kafka lifecycle configuration.
 * @throws When shared test cryptographic material fails local validation.
 */
function createValidatedMtlsConfig(): EmailServiceConfigService {
  const environment = validateEmailServiceEnv({
    PORT: '3000',
    DATABASE_URL: 'postgresql://user:password@localhost:5432/email',
    POSTGRES_SCHEMA: 'email',
    SENDGRID_API_KEY: 'test-api-key',
    EMAIL_FROM: 'noreply@example.com',
    EMAIL_TEMPLATE_MAP:
      '{"notifications.email":"d-one","billing.email":"d-two"}',
    KAFKA_URL: 'unreachable-broker.invalid:9092',
    KAFKA_CLIENT_ID: 'email-service-v6',
    KAFKA_GROUP_ID: 'email-workers',
    KAFKA_SSL_ENABLED: 'true',
    KAFKA_CLIENT_CERT: KAFKA_MTLS_CLIENT_CERT,
    KAFKA_CLIENT_CERT_KEY: KAFKA_MTLS_ENCRYPTED_CLIENT_KEY,
    KAFKA_CA_CERT: KAFKA_MTLS_CA_CERT,
    KAFKA_CLIENT_CERT_PASSPHRASE: KAFKA_MTLS_TEST_PASSPHRASE,
    KAFKA_CONNECTION_TIMEOUT: '100',
    KAFKA_REQUEST_TIMEOUT: '1000',
    KAFKA_BROKER_TIMEOUT: '100',
    KAFKA_SESSION_TIMEOUT: '5000',
    KAFKA_HEARTBEAT_INTERVAL: '500',
    KAFKA_RETRY_ATTEMPTS: '2',
    KAFKA_INITIAL_RETRY_TIME: '10',
    KAFKA_MAX_RETRY_TIME: '15',
    KAFKA_MAXBYTES: '2048',
    KAFKA_MAX_WAIT_TIME: '250',
    DISABLE_KAFKA: 'false',
  });

  return createConfig({
    brokers: environment.KAFKA_URL,
    clientId: environment.KAFKA_CLIENT_ID,
    groupId: environment.KAFKA_GROUP_ID,
    sslEnabled: environment.KAFKA_SSL_ENABLED,
    clientCert: environment.KAFKA_CLIENT_CERT,
    clientCertKey: environment.KAFKA_CLIENT_CERT_KEY,
    caCert: environment.KAFKA_CA_CERT,
    clientCertPassphrase: environment.KAFKA_CLIENT_CERT_PASSPHRASE,
  });
}

/**
 * Flushes queued promise reactions used by non-blocking lifecycle startup.
 *
 * @returns A promise resolved after several microtask turns.
 */
async function flushMicrotasks(): Promise<void> {
  for (let index = 0; index < 8; index += 1) {
    await Promise.resolve();
  }
}

/**
 * Creates one deterministic Kafka message for sequential-processing tests.
 *
 * @param offset - Offset identifying the message.
 * @returns A complete Kafka message.
 */
function createMessage(offset: bigint): KafkaMessage {
  return {
    topic: 'notifications.email',
    partition: 0,
    offset,
    timestamp: 1n,
    key: Buffer.from(`key-${offset}`),
    value: Buffer.from(`value-${offset}`),
  };
}

describe('KafkaConsumerService', () => {
  const createConsumer = jest.fn();
  const processMessage = jest.fn((): Promise<void> => Promise.resolve());
  let errorLog: jest.SpyInstance;

  beforeEach(() => {
    jest.useFakeTimers();
    jest.clearAllMocks();
    errorLog = jest
      .spyOn(LoggerService.prototype, 'error')
      .mockImplementation();
  });

  afterEach(() => {
    errorLog.mockRestore();
    jest.useRealTimers();
  });

  /**
   * Creates the service with shared factory and orchestrator test doubles.
   *
   * @param config - Config service for the lifecycle scenario.
   * @returns A fresh Kafka consumer lifecycle service.
   */
  function createService(
    config: EmailServiceConfigService,
  ): KafkaConsumerService {
    return new KafkaConsumerService(
      config,
      { createConsumer } as never,
      { processMessage } as never,
    );
  }

  it('starts initializing, subscribes to every mapped topic, and becomes ready', async () => {
    const stream = new TestMessageStream();
    const consumer = new TestConsumer(stream);
    createConsumer.mockResolvedValue(consumer);
    const service = createService(createConfig());

    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Initializing,
      reconnectAttempts: 0,
    });
    expect(service.onApplicationBootstrap()).toBeUndefined();
    await flushMicrotasks();

    expect(consumer.consume).toHaveBeenCalledWith({
      topics: ['notifications.email', 'billing.email'],
      autocommit: true,
      mode: 'committed',
      fallbackMode: 'latest',
      maxBytes: 2048,
      maxWaitTime: 250,
    });
    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Ready,
      reconnectAttempts: 0,
    });
    await service.onModuleDestroy();
  });

  it('initializes directly as disabled and creates no resources', () => {
    const service = createService(createConfig({ disabled: true }));

    service.onApplicationBootstrap();

    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Disabled,
      reconnectAttempts: 0,
    });
    expect(createConsumer).not.toHaveBeenCalled();
  });

  it('keeps startup non-fatal when the broker subscription fails', async () => {
    const stream = new TestMessageStream();
    const consumer = new TestConsumer(stream);
    consumer.consume.mockRejectedValueOnce(new Error('broker unavailable'));
    createConsumer.mockResolvedValue(consumer);
    const service = createService(createConfig());

    expect(service.onApplicationBootstrap()).toBeUndefined();
    await flushMicrotasks();

    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Reconnecting,
      reconnectAttempts: 1,
      failureReason: 'broker unavailable',
    });
    await service.onModuleDestroy();
  });

  it('starts immediately and reconnects when validated mTLS cannot reach a broker', async () => {
    const stream = new TestMessageStream();
    const consumer = new TestConsumer(stream);
    consumer.consume.mockRejectedValueOnce(
      new Error('unreachable broker connection refused'),
    );
    createConsumer.mockResolvedValue(consumer);
    const service = createService(createValidatedMtlsConfig());

    expect(service.onApplicationBootstrap()).toBeUndefined();
    await flushMicrotasks();

    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Reconnecting,
      reconnectAttempts: 1,
      failureReason: 'unreachable broker connection refused',
    });
    await service.onModuleDestroy();
  });

  it('recreates resources with committed offsets and resets status after success', async () => {
    const firstStream = new TestMessageStream();
    const firstConsumer = new TestConsumer(firstStream);
    const secondStream = new TestMessageStream();
    const secondConsumer = new TestConsumer(secondStream);
    createConsumer
      .mockResolvedValueOnce(firstConsumer)
      .mockResolvedValueOnce(secondConsumer);
    const service = createService(createConfig());
    service.onApplicationBootstrap();
    await flushMicrotasks();

    firstStream.emit('error', new Error('stream failed'));
    await flushMicrotasks();
    await jest.advanceTimersByTimeAsync(10);
    await flushMicrotasks();

    expect(createConsumer).toHaveBeenCalledTimes(2);
    expect(firstStream.close).toHaveBeenCalledTimes(1);
    expect(firstConsumer.close).toHaveBeenCalledTimes(1);
    expect(secondConsumer.consume).toHaveBeenCalledWith({
      topics: ['notifications.email', 'billing.email'],
      autocommit: true,
      mode: 'committed',
      fallbackMode: 'latest',
      maxBytes: 2048,
      maxWaitTime: 250,
    });
    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Ready,
      reconnectAttempts: 0,
    });
    await service.onModuleDestroy();
  });

  it('reconnects after an autocommit failure and ignores successful notices', async () => {
    const firstStream = new TestMessageStream();
    const firstConsumer = new TestConsumer(firstStream);
    const secondStream = new TestMessageStream();
    const secondConsumer = new TestConsumer(secondStream);
    createConsumer
      .mockResolvedValueOnce(firstConsumer)
      .mockResolvedValueOnce(secondConsumer);
    const service = createService(createConfig());
    service.onApplicationBootstrap();
    await flushMicrotasks();

    firstStream.emit('autocommit', null);
    await flushMicrotasks();
    expect(createConsumer).toHaveBeenCalledTimes(1);

    firstStream.emit('autocommit', new Error('offset commit failed'));
    await flushMicrotasks();
    await jest.advanceTimersByTimeAsync(10);
    await flushMicrotasks();

    expect(createConsumer).toHaveBeenCalledTimes(2);
    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Ready,
      reconnectAttempts: 0,
    });
    await service.onModuleDestroy();
  });

  it('rejects a replacement client that fails while its subscription settles', async () => {
    const firstStream = new TestMessageStream();
    const firstConsumer = new TestConsumer(firstStream);
    const candidateStream = new TestMessageStream();
    const candidateConsumer = new TestConsumer(candidateStream);
    let resolveCandidateSubscription!: (stream: KafkaMessageStream) => void;
    candidateConsumer.consume.mockImplementationOnce(
      () =>
        new Promise<KafkaMessageStream>((resolve) => {
          resolveCandidateSubscription = resolve;
        }),
    );
    const recoveredStream = new TestMessageStream();
    const recoveredConsumer = new TestConsumer(recoveredStream);
    createConsumer
      .mockResolvedValueOnce(firstConsumer)
      .mockResolvedValueOnce(candidateConsumer)
      .mockResolvedValueOnce(recoveredConsumer);
    const service = createService(createConfig());
    service.onApplicationBootstrap();
    await flushMicrotasks();

    firstStream.emit('error', new Error('initial stream failure'));
    await flushMicrotasks();
    await jest.advanceTimersByTimeAsync(10);
    await flushMicrotasks();

    candidateConsumer.emit('error', new Error('candidate client failure'));
    resolveCandidateSubscription(candidateStream);
    await flushMicrotasks();

    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Reconnecting,
      reconnectAttempts: 2,
      failureReason: 'candidate client failure',
    });
    expect(candidateStream.close).toHaveBeenCalledTimes(1);
    expect(candidateConsumer.close).toHaveBeenCalledTimes(1);

    await jest.advanceTimersByTimeAsync(15);
    await flushMicrotasks();

    expect(createConsumer).toHaveBeenCalledTimes(3);
    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Ready,
      reconnectAttempts: 0,
    });
    await service.onModuleDestroy();
  });

  it('allows broker fallback to complete without recreating the consumer', async () => {
    const stream = new TestMessageStream();
    const consumer = new TestConsumer(stream);
    let resolveSubscription!: (stream: KafkaMessageStream) => void;
    consumer.consume.mockImplementationOnce(
      () =>
        new Promise<KafkaMessageStream>((resolve) => {
          resolveSubscription = resolve;
        }),
    );
    createConsumer.mockResolvedValue(consumer);
    const service = createService(createConfig());
    service.onApplicationBootstrap();
    await flushMicrotasks();

    consumer.emit(
      'client:broker:failed',
      new Error('first broker unavailable'),
    );
    resolveSubscription(stream);
    await flushMicrotasks();

    expect(createConsumer).toHaveBeenCalledTimes(1);
    expect(consumer.close).not.toHaveBeenCalled();
    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Ready,
      reconnectAttempts: 0,
    });
    await service.onModuleDestroy();
  });

  it('enters failed state after the external reconnect budget is exhausted', async () => {
    createConsumer
      .mockRejectedValueOnce(new Error('initial failure'))
      .mockRejectedValueOnce(new Error('retry one failure'))
      .mockRejectedValueOnce(new Error('retry two failure'));
    const service = createService(createConfig());
    service.onApplicationBootstrap();
    await flushMicrotasks();

    await jest.advanceTimersByTimeAsync(10);
    await flushMicrotasks();
    await jest.advanceTimersByTimeAsync(15);
    await flushMicrotasks();

    expect(createConsumer).toHaveBeenCalledTimes(3);
    expect(service.getStatus()).toEqual({
      state: KafkaConnectionState.Failed,
      reconnectAttempts: 2,
      failureReason: 'retry two failure',
    });
    await service.onModuleDestroy();
  });

  it('shares one reconnect task across overlapping resource failures', async () => {
    const firstStream = new TestMessageStream();
    const firstConsumer = new TestConsumer(firstStream);
    const secondStream = new TestMessageStream();
    const secondConsumer = new TestConsumer(secondStream);
    createConsumer
      .mockResolvedValueOnce(firstConsumer)
      .mockResolvedValueOnce(secondConsumer);
    const service = createService(createConfig());
    service.onApplicationBootstrap();
    await flushMicrotasks();

    const queuedStreamError = firstStream.listeners('error')[0];
    firstConsumer.emit('error', new Error('client failed'));
    queuedStreamError(new Error('stream failed too'));
    await flushMicrotasks();
    await jest.advanceTimersByTimeAsync(10);
    await flushMicrotasks();

    expect(createConsumer).toHaveBeenCalledTimes(2);
    await service.onModuleDestroy();
  });

  it('processes stream messages sequentially', async () => {
    const stream = new TestMessageStream();
    const consumer = new TestConsumer(stream);
    createConsumer.mockResolvedValue(consumer);
    let releaseFirst!: () => void;
    processMessage.mockImplementationOnce(
      () =>
        new Promise<void>((resolve) => {
          releaseFirst = resolve;
        }),
    );
    const service = createService(createConfig());
    stream.pushMessage(createMessage(1n));
    stream.pushMessage(createMessage(2n));
    service.onApplicationBootstrap();
    await flushMicrotasks();

    expect(processMessage).toHaveBeenCalledTimes(1);

    releaseFirst();
    await flushMicrotasks();
    expect(processMessage).toHaveBeenCalledTimes(2);
    await service.onModuleDestroy();
  });

  it('waits for old-generation processing before shutdown completes', async () => {
    const firstStream = new TestMessageStream();
    const firstConsumer = new TestConsumer(firstStream);
    const secondStream = new TestMessageStream();
    const secondConsumer = new TestConsumer(secondStream);
    createConsumer
      .mockResolvedValueOnce(firstConsumer)
      .mockResolvedValueOnce(secondConsumer);
    let releaseOldProcessing!: () => void;
    processMessage.mockImplementationOnce(
      () =>
        new Promise<void>((resolve) => {
          releaseOldProcessing = resolve;
        }),
    );
    const service = createService(createConfig());
    firstStream.pushMessage(createMessage(1n));
    service.onApplicationBootstrap();
    await flushMicrotasks();

    firstStream.emit('error', new Error('stream failed'));
    await flushMicrotasks();
    await jest.advanceTimersByTimeAsync(10);
    await flushMicrotasks();

    expect(createConsumer).toHaveBeenCalledTimes(2);
    expect(service.getStatus().state).toBe(KafkaConnectionState.Ready);
    let shutdownCompleted = false;
    const shutdownPromise = service.onModuleDestroy().then(() => {
      shutdownCompleted = true;
    });
    await flushMicrotasks();

    expect(shutdownCompleted).toBe(false);
    releaseOldProcessing();
    await shutdownPromise;
    expect(shutdownCompleted).toBe(true);
  });

  it('cancels pending backoff and closes resources gracefully on shutdown', async () => {
    const stream = new TestMessageStream();
    const consumer = new TestConsumer(stream);
    consumer.consume.mockRejectedValueOnce(new Error('broker unavailable'));
    createConsumer.mockResolvedValue(consumer);
    const service = createService(createConfig());
    service.onApplicationBootstrap();
    await flushMicrotasks();

    await expect(service.onModuleDestroy()).resolves.toBeUndefined();

    expect(consumer.close).toHaveBeenCalledTimes(1);
    expect(jest.getTimerCount()).toBe(0);
    expect(createConsumer).toHaveBeenCalledTimes(1);
  });

  it('contains late errors after stream and consumer close failures', async () => {
    const stream = new TestMessageStream();
    stream.failClose(new Error('stream close failed'));
    const consumer = new TestConsumer(stream);
    consumer.close.mockRejectedValue(new Error('consumer close failed'));
    createConsumer.mockResolvedValue(consumer);
    const service = createService(createConfig());
    service.onApplicationBootstrap();
    await flushMicrotasks();

    await expect(service.onModuleDestroy()).resolves.toBeUndefined();

    expect(() =>
      stream.emit('error', new Error('late stream error')),
    ).not.toThrow();
    expect(() =>
      consumer.emit('error', new Error('late consumer error')),
    ).not.toThrow();
    expect(createConsumer).toHaveBeenCalledTimes(1);
    expect(jest.getTimerCount()).toBe(0);
  });

  it('uses equal jitter within each capped reconnect delay', () => {
    const service = createService(createConfig());
    const getReconnectDelay = (
      service as unknown as {
        getReconnectDelay(attempt: number): number;
      }
    ).getReconnectDelay.bind(service);
    const random = jest.spyOn(Math, 'random');

    random.mockReturnValueOnce(0);
    expect(getReconnectDelay(1)).toBe(5);
    random.mockReturnValueOnce(0.999_999);
    expect(getReconnectDelay(1)).toBe(10);
    random.mockReturnValueOnce(0.999_999);
    expect(getReconnectDelay(2)).toBe(15);

    random.mockRestore();
  });

  it('sanitizes secrets from reconnect status and exhaustion logs', async () => {
    createConsumer
      .mockRejectedValueOnce(
        new Error('failure PRIVATE-KEY-CONTENT PASSPHRASE-CONTENT'),
      )
      .mockRejectedValueOnce(new Error('failure CERTIFICATE-CONTENT'))
      .mockRejectedValueOnce(new Error('failure CA-CONTENT'));
    const service = createService(createConfig());
    service.onApplicationBootstrap();
    await flushMicrotasks();
    await jest.advanceTimersByTimeAsync(10);
    await flushMicrotasks();
    await jest.advanceTimersByTimeAsync(15);
    await flushMicrotasks();

    const serializedStatus = JSON.stringify(service.getStatus());
    expect(serializedStatus).not.toContain('PRIVATE-KEY-CONTENT');
    expect(serializedStatus).not.toContain('PASSPHRASE-CONTENT');
    expect(serializedStatus).not.toContain('CERTIFICATE-CONTENT');
    expect(serializedStatus).not.toContain('CA-CONTENT');
    await service.onModuleDestroy();
  });
});
