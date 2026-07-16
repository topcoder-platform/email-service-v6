import {
  Injectable,
  OnApplicationBootstrap,
  OnModuleDestroy,
} from '@nestjs/common';
import type { EmailAttempt } from '@prisma/client';
import { SchedulerRegistry } from '@nestjs/schedule';
import { CronJob } from 'cron';
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import type { NormalizedEmailPayload } from '../email-processing/email-processing.types';
import { SendGridDeliveryService } from '../email-processing/sendgrid-delivery.service';
import { LoggerService } from '../global/logger.service';
import { EmailAttemptRepository } from '../persistence/email-attempt.repository';

/** Stable persisted description for retry delivery failures. */
export const RETRY_DELIVERY_FAILURE_MESSAGE = 'SendGrid delivery failed';

/** Registry key for the dynamically configured retry cron job. */
export const EMAIL_RETRY_JOB_NAME = 'email-retry';

/**
 * Runs configured, process-local retry cycles for recent failed attempts while
 * preventing overlapping work within one application process.
 */
@Injectable()
export class EmailRetrySchedulerService
  implements OnApplicationBootstrap, OnModuleDestroy
{
  private readonly logger = LoggerService.forRoot('EmailRetrySchedulerService');
  private retryJob?: CronJob;
  private activeCycle?: Promise<void>;
  private teardownPromise?: Promise<void>;
  private acceptingTicks = true;

  /**
   * Creates the scheduler from validated configuration and shared boundaries.
   *
   * @param schedulerRegistry - Nest registry used to manage the dynamic job.
   * @param configService - Provider for the validated six-field cron schedule.
   * @param attemptRepository - Failed-attempt selection and status persistence.
   * @param deliveryService - Existing SendGrid delivery mapping and provider.
   * @throws Never; construction only stores injected collaborators.
   */
  constructor(
    private readonly schedulerRegistry: SchedulerRegistry,
    private readonly configService: EmailServiceConfigService,
    private readonly attemptRepository: EmailAttemptRepository,
    private readonly deliveryService: SendGridDeliveryService,
  ) {}

  /**
   * Registers and starts the validated retry cron during application startup.
   *
   * @returns Nothing after the job has been registered and started.
   * @throws Propagates invalid cron expressions so startup fails safely.
   */
  onApplicationBootstrap(): void {
    const job = new CronJob(this.configService.email.retryCron, () => {
      void this.runRetryCycle();
    });
    this.schedulerRegistry.addCronJob(EMAIL_RETRY_JOB_NAME, job);
    this.retryJob = job;
    job.start();
    this.logger.log({ event: 'email_retry_scheduler_started' });
  }

  /**
   * Idempotently stops new cron work during module teardown and drains any
   * retained retry cycle before imported persistence providers disconnect.
   *
   * @returns The shared teardown promise, resolved after scheduling and active
   * retry work have both stopped.
   * @throws {Error} After draining active work when cron cleanup fails.
   */
  onModuleDestroy(): Promise<void> {
    this.acceptingTicks = false;
    this.teardownPromise ??= this.stopAndDrain();
    return this.teardownPromise;
  }

  /**
   * Scans once for eligible failures and retries them sequentially.
   *
   * @returns A promise resolved after completion, a contained scan failure, or
   * an immediate skip during teardown or while another cycle is already active.
   * @throws Never; scan, delivery, and status-update failures are contained.
   */
  runRetryCycle(): Promise<void> {
    if (!this.acceptingTicks) {
      this.logger.log({ event: 'email_retry_cycle_skipped_shutdown' });
      return Promise.resolve();
    }

    if (this.activeCycle !== undefined) {
      this.logger.log({ event: 'email_retry_cycle_skipped_in_flight' });
      return Promise.resolve();
    }

    const activeCycle = this.executeRetryCycle().finally(() => {
      if (this.activeCycle === activeCycle) {
        this.activeCycle = undefined;
      }
    });
    this.activeCycle = activeCycle;
    return activeCycle;
  }

  /**
   * Stops local scheduling and waits for the retained delivery cycle to settle.
   *
   * @returns A promise resolved after no retry work can use persistence again.
   * @throws Propagates cron registry cleanup errors after active work drains.
   */
  private async stopAndDrain(): Promise<void> {
    let cleanupFailed = false;
    try {
      if (this.retryJob !== undefined) {
        await this.retryJob.stop();
        this.schedulerRegistry.deleteCronJob(EMAIL_RETRY_JOB_NAME);
        this.retryJob = undefined;
      }
    } catch {
      cleanupFailed = true;
    }

    await this.activeCycle;
    this.logger.log({ event: 'email_retry_scheduler_stopped' });

    if (cleanupFailed) {
      throw new Error('Email retry scheduler cleanup failed');
    }
  }

  /**
   * Scans for eligible failures and retries them sequentially.
   *
   * @returns A promise resolved after completion or a contained scan failure.
   * @throws Never; scan, delivery, and status-update failures are contained.
   */
  private async executeRetryCycle(): Promise<void> {
    let attempts: EmailAttempt[];
    try {
      attempts = await this.attemptRepository.findRetryableFailed();
    } catch {
      this.logger.error({ event: 'email_retry_scan_failed' });
      return;
    }

    for (const attempt of attempts) {
      await this.retryAttempt(attempt);
    }
  }

  /**
   * Delivers one persisted attempt and contains its delivery or update failure.
   *
   * @param attempt - Repository-selected failed attempt to retry unchanged.
   * @returns A promise resolved after the delivery and attempted status update.
   * @throws Never; provider and persistence failures are logged as safe events.
   */
  private async retryAttempt(attempt: EmailAttempt): Promise<void> {
    const attemptedAt = new Date();
    const retryCount = attempt.retryCount + 1;
    const metadata = { attemptId: attempt.id, retryCount };

    try {
      await this.deliveryService.send(
        attempt.templateId,
        attempt.payload as unknown as NormalizedEmailPayload,
      );
    } catch {
      this.logger.error({ event: 'email_retry_delivery_failed', ...metadata });
      try {
        await this.attemptRepository.markFailed(
          attempt.id,
          RETRY_DELIVERY_FAILURE_MESSAGE,
          attemptedAt,
          retryCount,
        );
      } catch {
        this.logger.error({
          event: 'email_retry_failure_persistence_failed',
          ...metadata,
        });
      }
      return;
    }

    try {
      await this.attemptRepository.markSuccess(
        attempt.id,
        attemptedAt,
        retryCount,
      );
      this.logger.log({ event: 'email_retry_succeeded', ...metadata });
    } catch {
      this.logger.error({
        event: 'email_retry_success_persistence_failed',
        ...metadata,
      });
    }
  }
}
