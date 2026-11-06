import { Module } from '@nestjs/common';
import {
  EmailSentPublisherService,
  EMAIL_SENT_PRODUCER_FACTORY,
} from './email-sent-publisher.service';
import {
  loadPlatformaticKafka,
  PLATFORMATIC_KAFKA_MODULE_LOADER,
  PlatformaticKafkaClientFactory,
} from './platformatic-kafka-client.factory';

/**
 * Provides the single email-sent publisher shared by initial Kafka delivery
 * and the failed-attempt retry scheduler.
 */
@Module({
  providers: [
    {
      provide: PLATFORMATIC_KAFKA_MODULE_LOADER,
      useValue: loadPlatformaticKafka,
    },
    PlatformaticKafkaClientFactory,
    {
      provide: EMAIL_SENT_PRODUCER_FACTORY,
      useExisting: PlatformaticKafkaClientFactory,
    },
    EmailSentPublisherService,
  ],
  exports: [EmailSentPublisherService],
})
export class EmailSentModule {}
