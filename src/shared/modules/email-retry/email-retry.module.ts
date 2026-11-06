import { Module } from '@nestjs/common';
import { EmailProcessingModule } from '../email-processing/email-processing.module';
import { EmailSentModule } from '../kafka/email-sent.module';
import { EmailAttemptPersistenceModule } from '../persistence/email-attempt-persistence.module';
import { EmailRetrySchedulerService } from './email-retry-scheduler.service';

/**
 * Registers the process-local failed-delivery retry scheduler with the shared
 * SendGrid delivery, email-attempt persistence, and email-sent event boundaries.
 */
@Module({
  imports: [
    EmailProcessingModule,
    EmailAttemptPersistenceModule,
    EmailSentModule,
  ],
  providers: [EmailRetrySchedulerService],
})
export class EmailRetryModule {}
