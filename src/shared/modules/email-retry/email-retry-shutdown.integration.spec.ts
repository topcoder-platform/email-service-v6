import { Global, INestApplication, Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { Test } from '@nestjs/testing';
import type { EmailAttempt } from '@prisma/client';
import { EmailAttemptStatus } from '@prisma/client';
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { SendGridDeliveryService } from '../email-processing/sendgrid-delivery.service';
import { EmailAttemptRepository } from '../persistence/email-attempt.repository';
import { PrismaService } from '../persistence/prisma.service';
import { EmailRetrySchedulerService } from './email-retry-scheduler.service';
import { EmailRetryModule } from './email-retry.module';

const TEST_CONFIG_SERVICE = {
  database: {
    url: 'postgresql://email:email@localhost:5432/email',
    schema: 'email',
  },
  email: {
    sendGridApiKey: 'SG.test.test',
    from: 'sender@example.com',
    retryCron: '0 0 0 1 1 *',
    retryMaxAgeMs: 86_400_000,
  },
} as EmailServiceConfigService;

/** Makes deterministic email configuration visible to the imported modules. */
@Global()
@Module({
  providers: [
    {
      provide: EmailServiceConfigService,
      useValue: TEST_CONFIG_SERVICE,
    },
  ],
  exports: [EmailServiceConfigService],
})
class ShutdownTestConfigModule {}

/** Reproduces the retry module's real persistence-import relationship. */
@Module({
  imports: [
    ShutdownTestConfigModule,
    ScheduleModule.forRoot(),
    EmailRetryModule,
  ],
})
class ShutdownTestApplicationModule {}

describe('email retry application shutdown', () => {
  let application: INestApplication | undefined;
  let resolveDelivery: (() => void) | undefined;
  let shutdownPromise: Promise<void> | undefined;

  afterEach(async () => {
    resolveDelivery?.();
    if (shutdownPromise !== undefined) {
      await shutdownPromise;
    } else if (application !== undefined) {
      await application.close();
    }
    jest.restoreAllMocks();
  });

  it('persists an active retry before Prisma disconnects and shutdown resolves', async () => {
    const now = new Date('2026-07-13T04:00:00.000Z');
    const attempt: EmailAttempt = {
      id: 'attempt-1',
      topic: 'notification.email',
      partition: 0,
      offset: '1',
      messageKey: null,
      messageTimestamp: now,
      templateId: 'd-persisted-template',
      payload: {
        recipients: ['recipient@example.com'],
        data: { firstName: 'Retry' },
      },
      recipients: ['recipient@example.com'],
      status: EmailAttemptStatus.FAILED,
      errorMessage: 'previous failure',
      retryCount: 1,
      lastAttemptedAt: now,
      sentAt: null,
      createdAt: now,
      updatedAt: now,
    };
    const shutdownEvents: string[] = [];
    const unresolvedDelivery = new Promise<never>((resolve) => {
      resolveDelivery = () => resolve([{}] as never);
    });
    jest
      .spyOn(PrismaService.prototype, '$connect')
      .mockResolvedValue(undefined);
    const disconnect = jest
      .spyOn(PrismaService.prototype, '$disconnect')
      .mockImplementation(() => {
        shutdownEvents.push('prisma-disconnected');
        return Promise.resolve();
      });
    jest
      .spyOn(EmailAttemptRepository.prototype, 'findRetryableFailed')
      .mockResolvedValue([attempt]);
    const markSuccess = jest
      .spyOn(EmailAttemptRepository.prototype, 'markSuccess')
      .mockImplementation(() => {
        shutdownEvents.push('retry-persisted');
        return Promise.resolve({
          ...attempt,
          status: EmailAttemptStatus.SUCCESS,
        });
      });
    const send = jest
      .spyOn(SendGridDeliveryService.prototype, 'send')
      .mockReturnValue(unresolvedDelivery);

    const testingModule = await Test.createTestingModule({
      imports: [ShutdownTestApplicationModule],
    }).compile();
    application = testingModule.createNestApplication();
    await application.init();

    const scheduler = application.get(EmailRetrySchedulerService);
    const activeCycle = scheduler.runRetryCycle();
    await Promise.resolve();

    expect(send).toHaveBeenCalledTimes(1);

    let shutdownResolved = false;
    shutdownPromise = application.close().then(() => {
      shutdownEvents.push('shutdown-resolved');
      shutdownResolved = true;
    });
    await Promise.resolve();

    expect(shutdownResolved).toBe(false);
    expect(disconnect).not.toHaveBeenCalled();
    expect(markSuccess).not.toHaveBeenCalled();

    resolveDelivery();
    await activeCycle;
    await shutdownPromise;

    expect(markSuccess).toHaveBeenCalledTimes(1);
    expect(disconnect).toHaveBeenCalledTimes(1);
    expect(shutdownEvents).toEqual([
      'retry-persisted',
      'prisma-disconnected',
      'shutdown-resolved',
    ]);
  });
});
