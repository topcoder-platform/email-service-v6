import { Module } from '@nestjs/common';
import { EmailProcessingModule } from '../email-processing/email-processing.module';
import { EmailAttemptPersistenceModule } from '../persistence/email-attempt-persistence.module';
import { EmailSentModule } from './email-sent.module';
import { KafkaConsumerService } from './kafka-consumer.service';
import { KafkaEmailOrchestratorService } from './kafka-email-orchestrator.service';

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
  // The Kafka loader and client factory come from EmailSentModule, so the
  // consumer and the email-sent producer share one instance of each.
  providers: [KafkaEmailOrchestratorService, KafkaConsumerService],
  exports: [KafkaConsumerService],
})
export class KafkaModule {}
