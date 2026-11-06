import { Inject, Injectable, OnModuleDestroy } from '@nestjs/common';
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { LoggerService } from '../global/logger.service';
import type { KafkaProducerClient, KafkaProducerFactory } from './kafka.types';

/** Injection token for the factory that creates the email-sent producer. */
export const EMAIL_SENT_PRODUCER_FACTORY = Symbol(
  'EMAIL_SENT_PRODUCER_FACTORY',
);

/** Originator recorded on email-sent Bus envelopes. */
export const EMAIL_SENT_ORIGINATOR = 'tc-email-service';

/** Facts about one accepted SendGrid send of a correlated email. */
export interface EmailSentEvent {
  /** Caller correlation id from the email payload (forms: `<submissionId>:<flowId>`). */
  correlationId: string;
  /** SendGrid `X-Message-Id`, or null when SendGrid did not return one. */
  sendgridMessageId: string | null;
  /** SendGrid template used for the send. */
  templateId: string;
  /** Time of the accepted provider call. */
  sentAt: Date;
}

/**
 * Publishes an email-sent Bus envelope after each accepted send of an email
 * that carries a correlation id, so callers can link their records to the
 * SendGrid message. Emails without a correlation id publish nothing.
 */
@Injectable()
export class EmailSentPublisherService implements OnModuleDestroy {
  private readonly logger = LoggerService.forRoot('EmailSentPublisherService');
  private producer?: Promise<KafkaProducerClient>;

  /**
   * Creates the publisher from validated configuration and a producer factory.
   *
   * @param configService - Provider for the Kafka switch and email-sent topic.
   * @param producerFactory - Factory used once to create the shared producer.
   * @throws Never; the producer is created lazily on the first publish.
   */
  constructor(
    private readonly configService: EmailServiceConfigService,
    @Inject(EMAIL_SENT_PRODUCER_FACTORY)
    private readonly producerFactory: KafkaProducerFactory,
  ) {}

  /**
   * Publishes one email-sent event when Kafka and the topic are enabled.
   *
   * @param event - Correlation id, SendGrid message id, template, and send time.
   * @returns A promise resolved after the event is accepted, skipped, or the
   * failure is logged.
   * @remarks Called by the initial and retry delivery paths after a send succeeds.
   * The attempt is already sent, so publish failures never change its status.
   * @throws Never; producer creation and send failures are logged without content.
   */
  async publish(event: EmailSentEvent): Promise<void> {
    const kafka = this.configService.kafka;
    const topic = kafka.emailSentTopic;
    if (kafka.disabled || !topic) {
      return;
    }

    const value = JSON.stringify({
      topic,
      originator: EMAIL_SENT_ORIGINATOR,
      timestamp: new Date().toISOString(),
      'mime-type': 'application/json',
      payload: {
        correlationId: event.correlationId,
        sendgridMessageId: event.sendgridMessageId,
        templateId: event.templateId,
        sentAt: event.sentAt.toISOString(),
      },
    });
    try {
      const producer = await this.getProducer();
      await producer.send({
        messages: [
          {
            topic,
            key: Buffer.from(event.correlationId, 'utf8'),
            value: Buffer.from(value, 'utf8'),
          },
        ],
      });
      this.logger.log({
        event: 'email_sent_event_published',
        topic,
        hasMessageId: event.sendgridMessageId !== null,
      });
    } catch {
      this.logger.error({ event: 'email_sent_event_publish_failed', topic });
    }
  }

  /**
   * Closes the shared producer during application shutdown.
   *
   * @returns A promise resolved after the producer closes or was never created.
   * @throws Never; close failures are logged.
   */
  async onModuleDestroy(): Promise<void> {
    const producer = this.producer;
    this.producer = undefined;
    if (producer === undefined) {
      return;
    }
    try {
      await (await producer).close();
    } catch {
      this.logger.error({ event: 'email_sent_producer_close_failed' });
    }
  }

  /**
   * Creates the producer once and shares it, retrying creation after a failure.
   *
   * @returns The shared producer.
   * @throws Propagates creation failures to publish, which logs them.
   */
  private getProducer(): Promise<KafkaProducerClient> {
    if (this.producer === undefined) {
      const created = this.producerFactory.createProducer();
      this.producer = created;
      created.catch(() => {
        if (this.producer === created) {
          this.producer = undefined;
        }
      });
    }
    return this.producer;
  }
}
