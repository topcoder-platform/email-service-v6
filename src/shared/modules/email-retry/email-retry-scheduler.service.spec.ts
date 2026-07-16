import type { EmailAttempt } from '@prisma/client';
import { EmailAttemptStatus } from '@prisma/client';
import type { SchedulerRegistry } from '@nestjs/schedule';
import type { EmailServiceConfigService } from '../../../config/email-service-config.service';
import type { SendGridDeliveryService } from '../email-processing/sendgrid-delivery.service';
import type { EmailAttemptRepository } from '../persistence/email-attempt.repository';
import {
  EMAIL_RETRY_JOB_NAME,
  EmailRetrySchedulerService,
  RETRY_DELIVERY_FAILURE_MESSAGE,
} from './email-retry-scheduler.service';

/**
 * Creates a complete failed attempt for retry scheduler tests.
 *
 * @param overrides - Attempt fields replaced for a specific test scenario.
 * @returns A failed persisted attempt with normalized payload JSON.
 * @throws Never; the helper only merges deterministic test data.
 */
function createAttempt(overrides: Partial<EmailAttempt> = {}): EmailAttempt {
  const now = new Date('2026-07-13T04:00:00.000Z');
  return {
    id: 'attempt-1',
    topic: 'notification.email',
    partition: 0,
    offset: '1',
    messageKey: null,
    messageTimestamp: now,
    templateId: 'd-persisted-template',
    payload: {
      recipients: ['secret-recipient@example.com'],
      data: { privateValue: 'secret-payload-value' },
    },
    recipients: ['secret-recipient@example.com'],
    status: EmailAttemptStatus.FAILED,
    errorMessage: 'previous failure',
    retryCount: 1,
    lastAttemptedAt: now,
    sentAt: null,
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

describe('EmailRetrySchedulerService', () => {
  const addCronJob = jest.fn();
  const deleteCronJob = jest.fn();
  const schedulerRegistry = {
    addCronJob,
    deleteCronJob,
  } as unknown as SchedulerRegistry;
  const configService = {
    email: { retryCron: '0 */2 * * * *' },
  } as EmailServiceConfigService;
  const findRetryableFailed = jest.fn();
  const markSuccess = jest.fn();
  const markFailed = jest.fn();
  const attemptRepository = {
    findRetryableFailed,
    markSuccess,
    markFailed,
  } as unknown as EmailAttemptRepository;
  const send = jest.fn();
  const deliveryService = {
    send,
  } as unknown as SendGridDeliveryService;
  let service: EmailRetrySchedulerService;

  beforeEach(() => {
    jest.clearAllMocks();
    findRetryableFailed.mockResolvedValue([]);
    markSuccess.mockResolvedValue(createAttempt());
    markFailed.mockResolvedValue(createAttempt());
    send.mockResolvedValue([{}] as never);
    service = new EmailRetrySchedulerService(
      schedulerRegistry,
      configService,
      attemptRepository,
      deliveryService,
    );
  });

  it('registers, starts, stops, and unregisters the configured dynamic job', async () => {
    service.onApplicationBootstrap();

    expect(addCronJob).toHaveBeenCalledWith(
      EMAIL_RETRY_JOB_NAME,
      expect.any(Object),
    );

    await service.onModuleDestroy();

    expect(deleteCronJob).toHaveBeenCalledWith(EMAIL_RETRY_JOB_NAME);
  });

  it('shares one teardown operation across repeated destroy hooks', async () => {
    service.onApplicationBootstrap();

    const firstTeardown = service.onModuleDestroy();
    const secondTeardown = service.onModuleDestroy();

    expect(secondTeardown).toBe(firstTeardown);
    await Promise.all([firstTeardown, secondTeardown]);

    expect(deleteCronJob).toHaveBeenCalledTimes(1);
  });

  it('sends only repository-returned attempts using persisted delivery data', async () => {
    const first = createAttempt();
    const second = createAttempt({
      id: 'attempt-2',
      templateId: 'd-second-template',
      payload: { recipients: ['second@example.com'], data: { code: 123 } },
    });
    findRetryableFailed.mockResolvedValue([first, second]);

    await service.runRetryCycle();

    expect(findRetryableFailed).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenNthCalledWith(1, first.templateId, first.payload);
    expect(send).toHaveBeenNthCalledWith(2, second.templateId, second.payload);
    expect(send).toHaveBeenCalledTimes(2);
  });

  it('persists a successful retry with one timestamp and incremented count', async () => {
    const attempt = createAttempt({ retryCount: 4 });
    findRetryableFailed.mockResolvedValue([attempt]);

    await service.runRetryCycle();

    expect(markSuccess).toHaveBeenCalledWith(attempt.id, expect.any(Date), 5);
    expect(markFailed).not.toHaveBeenCalled();
  });

  it('persists a stable failure and incremented count after rejection', async () => {
    const attempt = createAttempt({ retryCount: 2 });
    findRetryableFailed.mockResolvedValue([attempt]);
    send.mockRejectedValue(new Error('raw provider secret'));

    await service.runRetryCycle();

    expect(markFailed).toHaveBeenCalledWith(
      attempt.id,
      RETRY_DELIVERY_FAILURE_MESSAGE,
      expect.any(Date),
      3,
    );
    expect(markSuccess).not.toHaveBeenCalled();
  });

  it.each(['delivery', 'status update'])(
    'continues after an individual %s failure',
    async (failureType) => {
      const first = createAttempt();
      const second = createAttempt({ id: 'attempt-2' });
      findRetryableFailed.mockResolvedValue([first, second]);
      if (failureType === 'delivery') {
        send
          .mockRejectedValueOnce(new Error('provider failure'))
          .mockResolvedValueOnce([{}] as never);
      } else {
        markSuccess
          .mockRejectedValueOnce(new Error('database failure'))
          .mockResolvedValueOnce(second);
      }

      await service.runRetryCycle();

      expect(send).toHaveBeenCalledTimes(2);
      expect(markSuccess).toHaveBeenCalledWith(
        second.id,
        expect.any(Date),
        second.retryCount + 1,
      );
    },
  );

  it('contains repository scan failures safely', async () => {
    findRetryableFailed.mockRejectedValue(
      new Error('database connection details'),
    );

    await expect(service.runRetryCycle()).resolves.toBeUndefined();

    expect(send).not.toHaveBeenCalled();
  });

  it('does not overlap an unresolved cycle', async () => {
    const attempt = createAttempt();
    let resolveDelivery: ((value: never) => void) | undefined;
    const unresolvedDelivery = new Promise<never>((resolve) => {
      resolveDelivery = resolve;
    });
    findRetryableFailed.mockResolvedValue([attempt]);
    send.mockReturnValue(unresolvedDelivery);

    const firstCycle = service.runRetryCycle();
    await Promise.resolve();
    await service.runRetryCycle();

    expect(findRetryableFailed).toHaveBeenCalledTimes(1);
    resolveDelivery?.([{}] as never);
    await firstCycle;
  });

  it('waits for an active delivery before module teardown finishes', async () => {
    const attempt = createAttempt();
    let resolveDelivery: ((value: never) => void) | undefined;
    const unresolvedDelivery = new Promise<never>((resolve) => {
      resolveDelivery = resolve;
    });
    findRetryableFailed.mockResolvedValue([attempt]);
    send.mockReturnValue(unresolvedDelivery);

    const activeCycle = service.runRetryCycle();
    await Promise.resolve();

    let teardownFinished = false;
    const teardown = service.onModuleDestroy().then(() => {
      teardownFinished = true;
    });
    await Promise.resolve();

    expect(teardownFinished).toBe(false);
    expect(markSuccess).not.toHaveBeenCalled();

    resolveDelivery?.([{}] as never);
    await teardown;
    await activeCycle;

    expect(markSuccess).toHaveBeenCalledTimes(1);
    expect(teardownFinished).toBe(true);

    await service.runRetryCycle();
    expect(findRetryableFailed).toHaveBeenCalledTimes(1);
    expect(markSuccess).toHaveBeenCalledTimes(1);
  });

  it.each(['successful', 'failed'])(
    'releases the in-flight guard after a %s cycle',
    async (outcome) => {
      if (outcome === 'failed') {
        findRetryableFailed.mockRejectedValueOnce(new Error('scan failure'));
      }

      await service.runRetryCycle();
      await service.runRetryCycle();

      expect(findRetryableFailed).toHaveBeenCalledTimes(2);
    },
  );

  it('does not log payloads, recipients, or raw provider errors', async () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation();
    const errorSpy = jest.spyOn(console, 'error').mockImplementation();
    findRetryableFailed.mockResolvedValue([createAttempt()]);
    send.mockRejectedValue(
      new Error('raw-provider-error secret-payload-value'),
    );

    await service.runRetryCycle();

    const output = [...logSpy.mock.calls, ...errorSpy.mock.calls]
      .flat()
      .join(' ');
    expect(output).not.toContain('secret-recipient@example.com');
    expect(output).not.toContain('secret-payload-value');
    expect(output).not.toContain('raw-provider-error');

    logSpy.mockRestore();
    errorSpy.mockRestore();
  });
});
