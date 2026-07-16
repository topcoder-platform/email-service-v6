import { Module } from '@nestjs/common';
import { EmailMessageNormalizer } from './email-message-normalizer.service';
import { EmailProcessingService } from './email-processing.service';
import { EmailTemplateResolver } from './email-template-resolver.service';
import { SendGridDeliveryService } from './sendgrid-delivery.service';

/**
 * Registers normalized message preparation and the shared SendGrid delivery
 * boundary used by both Kafka initial delivery and persisted retry processing.
 * Normalization and template resolution remain internal implementation details.
 */
@Module({
  providers: [
    EmailMessageNormalizer,
    EmailTemplateResolver,
    EmailProcessingService,
    SendGridDeliveryService,
  ],
  exports: [EmailProcessingService, SendGridDeliveryService],
})
export class EmailProcessingModule {}
