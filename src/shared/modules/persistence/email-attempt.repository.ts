import { Injectable } from '@nestjs/common';
import { EmailAttempt, EmailAttemptStatus, Prisma } from '@prisma/client';
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { PrismaService } from './prisma.service';

/** Input required to persist a pending email delivery attempt. */
export interface CreatePendingEmailAttemptInput {
  topic: string;
  partition?: number;
  offset?: string;
  messageKey?: string;
  messageTimestamp?: Date;
  templateId: string;
  payload: Prisma.InputJsonValue;
  recipients: Prisma.InputJsonValue;
}

/**
 * Encapsulates persistence operations for email delivery attempts, including
 * status transitions and bounded failed-attempt retry selection.
 */
@Injectable()
export class EmailAttemptRepository {
  /**
   * Creates the repository with its Prisma client and retry configuration.
   *
   * @param prisma - Email-service Prisma client.
   * @param configService - Validated email-service configuration provider.
   */
  constructor(
    private readonly prisma: PrismaService,
    private readonly configService: EmailServiceConfigService,
  ) {}

  /**
   * Persists a new pending attempt and its optional Kafka correlation metadata.
   *
   * @param input - Normalized delivery and Kafka metadata to persist.
   * @returns The newly created email attempt.
   * @throws Propagates Prisma persistence errors.
   */
  createPending(input: CreatePendingEmailAttemptInput): Promise<EmailAttempt> {
    return this.prisma.emailAttempt.create({
      data: {
        ...input,
        status: EmailAttemptStatus.PENDING,
      },
    });
  }

  /**
   * Marks an attempt as successfully sent and clears its latest failure.
   *
   * @param id - Email attempt identifier.
   * @param attemptedAt - Time of the successful provider call.
   * @param retryCount - Optional exact retry count owned by the retry caller.
   * @returns The updated email attempt.
   * @throws Propagates Prisma persistence errors, including a missing record.
   */
  markSuccess(
    id: string,
    attemptedAt: Date = new Date(),
    retryCount?: number,
  ): Promise<EmailAttempt> {
    return this.prisma.emailAttempt.update({
      where: { id },
      data: {
        status: EmailAttemptStatus.SUCCESS,
        errorMessage: null,
        sentAt: attemptedAt,
        lastAttemptedAt: attemptedAt,
        ...(retryCount === undefined ? {} : { retryCount }),
      },
    });
  }

  /**
   * Marks an attempt as failed while allowing a retry caller to explicitly
   * persist its current retry count.
   *
   * @param id - Email attempt identifier.
   * @param errorMessage - Latest delivery failure message.
   * @param attemptedAt - Time of the failed provider call.
   * @param retryCount - Optional exact retry count owned by the retry caller.
   * @returns The updated email attempt.
   * @throws Propagates Prisma persistence errors, including a missing record.
   */
  markFailed(
    id: string,
    errorMessage: string,
    attemptedAt: Date = new Date(),
    retryCount?: number,
  ): Promise<EmailAttempt> {
    return this.prisma.emailAttempt.update({
      where: { id },
      data: {
        status: EmailAttemptStatus.FAILED,
        errorMessage,
        lastAttemptedAt: attemptedAt,
        ...(retryCount === undefined ? {} : { retryCount }),
      },
    });
  }

  /**
   * Finds failed attempts created within the configured maximum retry age.
   * Pending attempts are deliberately excluded from this query.
   *
   * @param now - Current time used to calculate the retry-age cutoff.
   * @returns Failed attempts ordered from oldest to newest creation time.
   * @throws Propagates Prisma query errors.
   */
  findRetryableFailed(now: Date = new Date()): Promise<EmailAttempt[]> {
    const cutoff = new Date(
      now.getTime() - this.configService.email.retryMaxAgeMs,
    );

    return this.prisma.emailAttempt.findMany({
      where: {
        status: EmailAttemptStatus.FAILED,
        createdAt: { gte: cutoff },
      },
      orderBy: [{ createdAt: 'asc' }, { id: 'asc' }],
    });
  }
}
