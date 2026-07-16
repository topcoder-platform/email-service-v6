import { Module } from '@nestjs/common';
import { EmailProcessingModule } from '../email-processing/email-processing.module';
import { EmailAttemptPersistenceModule } from '../persistence/email-attempt-persistence.module';
import { EmailRetrySchedulerService } from './email-retry-scheduler.service';

/**
 * Registers the process-local failed-delivery retry scheduler with the shared
 * SendGrid delivery and email-attempt persistence boundaries.
 */
@Module({
  imports: [EmailProcessingModule, EmailAttemptPersistenceModule],
  providers: [EmailRetrySchedulerService],
})
export class EmailRetryModule {}
