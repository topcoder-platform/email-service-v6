import { Injectable } from '@nestjs/common';
import type { Prisma } from '@prisma/client';
import { EmailProcessingService } from '../email-processing/email-processing.service';
import { SendGridDeliveryService } from '../email-processing/sendgrid-delivery.service';
import { LoggerService } from '../global/logger.service';
import { EmailAttemptRepository } from '../persistence/email-attempt.repository';
import type { KafkaMessage } from './kafka.types';

interface KafkaCorrelationMetadata {
  topic: string;
  partition: number;
  offset: string;
  hasMessageKey: boolean;
}

/**
 * Coordinates preparation, attempt persistence, and SendGrid delivery for one
 * Kafka message while containing expected collaborator failures.
 */
@Injectable()
export class KafkaEmailOrchestratorService {
  private readonly logger = LoggerService.forRoot(
    'KafkaEmailOrchestratorService',
  );

  /**
   * Creates the Kafka-to-email orchestration boundary.
   *
   * @param processingService - Safe normalization and template preparation.
   * @param deliveryService - SendGrid provider boundary.
   * @param attemptRepository - Persistence boundary for delivery attempts.
   * @throws Never; construction only stores injected collaborators.
   */
  constructor(
    private readonly processingService: EmailProcessingService,
    private readonly deliveryService: SendGridDeliveryService,
    private readonly attemptRepository: EmailAttemptRepository,
  ) {}

  /**
   * Processes one Kafka message sequentially through preparation and delivery.
   *
   * @param message - Kafka value and correlation metadata to process.
   * @returns A promise resolved after skip, delivery, or contained failure.
   * @throws Never for preparation, persistence, or SendGrid failures.
   */
  async processMessage(message: KafkaMessage): Promise<void> {
    const correlation = this.getCorrelationMetadata(message);
    let preparation: ReturnType<EmailProcessingService['prepare']>;
    try {
      preparation = this.processingService.prepare(
        message.topic,
        message.value.toString('utf8'),
      );
    } catch {
      this.logger.error({
        event: 'email_preparation_failed_unexpectedly',
        ...correlation,
      });
      return;
    }

    if (!preparation.ready) {
      return;
    }

    let attemptId: string | undefined;
    try {
      const attempt = await this.attemptRepository.createPending({
        topic: message.topic,
        partition: message.partition,
        offset: message.offset.toString(),
        ...(message.key === undefined || message.key === null
          ? {}
          : { messageKey: message.key.toString('utf8') }),
        ...(this.toValidDate(message.timestamp) === undefined
          ? {}
          : { messageTimestamp: this.toValidDate(message.timestamp) }),
        templateId: preparation.templateId,
        payload: preparation.payload as Prisma.InputJsonValue,
        recipients: preparation.payload.recipients as Prisma.InputJsonValue,
      });
      attemptId = attempt.id;
    } catch {
      this.logger.error({
        event: 'email_pending_persistence_failed',
        severity: 'critical',
        ...correlation,
      });
    }

    try {
      await this.deliveryService.send(
        preparation.templateId,
        preparation.payload,
      );
      if (attemptId !== undefined) {
        await this.markSuccess(attemptId, correlation);
      }
    } catch {
      this.logger.error({
        event: 'email_delivery_failed',
        ...correlation,
      });
      if (attemptId !== undefined) {
        await this.markFailed(attemptId, correlation);
      }
    }
  }

  /**
   * Marks a persisted attempt successful while containing update failures.
   *
   * @param attemptId - Persisted email-attempt identifier.
   * @param correlation - Safe Kafka metadata used in structured logging.
   * @returns A promise resolved after the update or contained failure.
   * @throws Never; repository errors are logged without sensitive content.
   */
  private async markSuccess(
    attemptId: string,
    correlation: KafkaCorrelationMetadata,
  ): Promise<void> {
    try {
      await this.attemptRepository.markSuccess(attemptId);
    } catch {
      this.logger.error({
        event: 'email_success_persistence_failed',
        ...correlation,
      });
    }
  }

  /**
   * Marks a persisted attempt failed using a stable, payload-free description.
   *
   * @param attemptId - Persisted email-attempt identifier.
   * @param correlation - Safe Kafka metadata used in structured logging.
   * @returns A promise resolved after the update or contained failure.
   * @throws Never; repository errors are logged without sensitive content.
   */
  private async markFailed(
    attemptId: string,
    correlation: KafkaCorrelationMetadata,
  ): Promise<void> {
    try {
      await this.attemptRepository.markFailed(
        attemptId,
        'SendGrid delivery failed',
      );
    } catch {
      this.logger.error({
        event: 'email_failure_persistence_failed',
        ...correlation,
      });
    }
  }

  /**
   * Extracts correlation fields while deliberately excluding message content.
   *
   * @param message - Kafka message whose metadata is required for logging.
   * @returns Topic, partition, offset, and whether a key was supplied.
   * @throws Never for a typed Kafka message.
   */
  private getCorrelationMetadata(
    message: KafkaMessage,
  ): KafkaCorrelationMetadata {
    return {
      topic: message.topic,
      partition: message.partition,
      offset: message.offset.toString(),
      hasMessageKey: message.key !== undefined && message.key !== null,
    };
  }

  /**
   * Converts a Kafka millisecond timestamp only when Node can represent it.
   *
   * @param timestamp - Kafka timestamp expressed as a bigint.
   * @returns A valid Date, or undefined when outside the ECMAScript date range.
   * @throws Never; range validation occurs before Date construction.
   */
  private toValidDate(timestamp: bigint): Date | undefined {
    const maximumDateMilliseconds = 8_640_000_000_000_000n;
    if (
      timestamp < -maximumDateMilliseconds ||
      timestamp > maximumDateMilliseconds
    ) {
      return undefined;
    }
    const date = new Date(Number(timestamp));
    return Number.isNaN(date.getTime()) ? undefined : date;
  }
}
