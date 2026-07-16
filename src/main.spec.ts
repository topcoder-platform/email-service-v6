import type { NestExpressApplication } from '@nestjs/platform-express';
import { NestFactory } from '@nestjs/core';
import type { EmailServiceConfigService } from './config/email-service-config.service';
import { KAFKA_MTLS_VALIDATION_ERROR } from './config/email-service-env';
import { bootstrap, startApplication } from './main';
import { LoggerService } from './shared/modules/global/logger.service';
import { KafkaConsumerService } from './shared/modules/kafka/kafka-consumer.service';
import { KafkaConnectionState } from './shared/modules/kafka/kafka.types';

jest.mock('./app.module', () => ({
  AppModule: class AppModule {},
}));

jest.mock('@nestjs/core', () => ({
  NestFactory: {
    create: jest.fn(),
  },
}));

describe('application bootstrap', () => {
  // NestFactory.create is a static factory and does not use a bound `this`.
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const createApplication = jest.mocked(NestFactory.create);
  const previousExitCode = process.exitCode;
  let log: jest.SpyInstance;
  let errorLog: jest.SpyInstance;

  beforeEach(() => {
    log = jest.spyOn(LoggerService.prototype, 'log').mockImplementation();
    errorLog = jest
      .spyOn(LoggerService.prototype, 'error')
      .mockImplementation();
  });

  afterEach(() => {
    jest.clearAllMocks();
    log.mockRestore();
    errorLog.mockRestore();
    process.exitCode = previousExitCode;
  });

  it('reaches HTTP listening while Kafka connection work remains asynchronous', async () => {
    const pendingKafkaConnection = new Promise<never>(() => undefined);
    const createConsumer = jest.fn(() => pendingKafkaConnection);
    const kafkaConsumer = new KafkaConsumerService(
      {
        email: { templateMap: { 'notifications.email': 'd-one' } },
        kafka: {
          disabled: false,
          retryAttempts: 2,
          initialRetryTime: 10,
          maxRetryTime: 15,
          maxBytes: 2048,
          maxWaitTime: 250,
        },
      } as EmailServiceConfigService,
      { createConsumer } as never,
      { processMessage: jest.fn() } as never,
    );
    const listen = jest.fn(() => {
      expect(kafkaConsumer.onApplicationBootstrap()).toBeUndefined();
      return Promise.resolve();
    });
    const app = {
      setGlobalPrefix: jest.fn(),
      enableCors: jest.fn(),
      useBodyParser: jest.fn(),
      useGlobalPipes: jest.fn(),
      enableShutdownHooks: jest.fn(),
      get: jest.fn().mockReturnValue({ app: { port: 3000 } }),
      listen,
      close: jest.fn(),
    } as unknown as NestExpressApplication;
    createApplication.mockResolvedValue(app);

    await expect(bootstrap()).resolves.toBeUndefined();

    expect(listen).toHaveBeenCalledWith(3000);
    expect(createConsumer).toHaveBeenCalledTimes(1);
    expect(kafkaConsumer.getStatus()).toEqual({
      state: KafkaConnectionState.Initializing,
      reconnectAttempts: 0,
    });
    expect(log.mock.calls).toEqual([
      ['Validating email service configuration'],
      ['Email service configuration validated'],
      ['CORS, body parsers, and validation pipe configured'],
      ['Email service started on port 3000'],
    ]);
    expect(errorLog).not.toHaveBeenCalled();
  });

  it('closes initialized resources and records failure when listening rejects', async () => {
    const listenerFailure = new Error(
      'listener failure containing database credentials',
    );
    const close = jest.fn().mockResolvedValue(undefined);
    const listen = jest.fn().mockRejectedValue(listenerFailure);
    const app = {
      setGlobalPrefix: jest.fn(),
      enableCors: jest.fn(),
      useBodyParser: jest.fn(),
      useGlobalPipes: jest.fn(),
      enableShutdownHooks: jest.fn(),
      get: jest.fn().mockReturnValue({ app: { port: 3000 } }),
      listen,
      close,
    } as unknown as NestExpressApplication;
    createApplication.mockResolvedValue(app);
    process.exitCode = undefined;

    await startApplication();

    expect(listen).toHaveBeenCalledWith(3000);
    expect(close).toHaveBeenCalledTimes(1);
    expect(process.exitCode).toBe(1);
  });

  it('preserves the startup failure when application cleanup also fails', async () => {
    const listenerFailure = new Error('original listener failure');
    const app = {
      setGlobalPrefix: jest.fn(),
      enableCors: jest.fn(),
      useBodyParser: jest.fn(),
      useGlobalPipes: jest.fn(),
      enableShutdownHooks: jest.fn(),
      get: jest.fn().mockReturnValue({ app: { port: 3000 } }),
      listen: jest.fn().mockRejectedValue(listenerFailure),
      close: jest.fn().mockRejectedValue(new Error('cleanup failure')),
    } as unknown as NestExpressApplication;
    createApplication.mockResolvedValue(app);

    await expect(bootstrap()).rejects.toBe(listenerFailure);
  });

  it('logs only stable startup events when mTLS configuration is rejected', async () => {
    const rejectedDetails = [
      KAFKA_MTLS_VALIDATION_ERROR,
      '-----BEGIN PRIVATE KEY-----',
      'startup-test-passphrase',
      'postgresql://user:password@private-database/email',
      'error:1C800064:Provider routines::bad decrypt',
    ].join(' ');
    createApplication.mockRejectedValue(new Error(rejectedDetails));
    process.exitCode = undefined;

    await startApplication();

    expect(log.mock.calls).toEqual([
      ['Validating email service configuration'],
    ]);
    expect(errorLog.mock.calls).toEqual([
      [{ event: 'application_creation_failed' }],
      [{ event: 'email_service_startup_failed' }],
    ]);
    const startupLogs = JSON.stringify([
      ...log.mock.calls,
      ...errorLog.mock.calls,
    ]);
    for (const rejectedDetail of [
      '-----BEGIN PRIVATE KEY-----',
      'startup-test-passphrase',
      'postgresql://user:password@private-database/email',
      'error:1C800064:Provider routines::bad decrypt',
    ]) {
      expect(startupLogs).not.toContain(rejectedDetail);
    }
    expect(process.exitCode).toBe(1);
  });
});
