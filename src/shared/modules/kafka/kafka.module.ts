import { Module } from '@nestjs/common';
import { EmailProcessingModule } from '../email-processing/email-processing.module';
import { EmailAttemptPersistenceModule } from '../persistence/email-attempt-persistence.module';
import { EmailSentModule } from './email-sent.module';
import { KafkaConsumerService } from './kafka-consumer.service';
import { KafkaEmailOrchestratorService } from './kafka-email-orchestrator.service';
import {
  loadPlatformaticKafka,
  PLATFORMATIC_KAFKA_MODULE_LOADER,
  PlatformaticKafkaClientFactory,
} from './platformatic-kafka-client.factory';

/**
 * Registers Kafka client construction, email orchestration (including the
 * email-sent event), and consumer lifecycle services, exporting status access for future health reporting.
 */
@Module({
  imports: [
    EmailProcessingModule,
    EmailAttemptPersistenceModule,
    EmailSentModule,
  ],
  providers: [
    {
      provide: PLATFORMATIC_KAFKA_MODULE_LOADER,
      useValue: loadPlatformaticKafka,
    },
    PlatformaticKafkaClientFactory,
    KafkaEmailOrchestratorService,
    KafkaConsumerService,
  ],
  exports: [KafkaConsumerService],
})
export class KafkaModule {}
