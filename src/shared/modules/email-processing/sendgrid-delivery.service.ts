import { Injectable } from '@nestjs/common';
import { MailDataRequired } from '@sendgrid/mail';
// The SDK uses `export =`; a default import emits an undefined `.default` here.
// eslint-disable-next-line @typescript-eslint/no-require-imports
import sendGridMail = require('@sendgrid/mail');
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { LoggerService } from '../global/logger.service';
import {
  EmailRecipient,
  NormalizedEmailPayload,
} from './email-processing.types';

/**
 * Delivers normalized messages through the configured SendGrid client.
 * Kafka orchestration and retry scheduling share this provider so persisted
 * retries use exactly the same legacy and v3 field mappings as initial sends.
 */
@Injectable()
export class SendGridDeliveryService {
  private readonly logger = LoggerService.forRoot('SendGridDeliveryService');
  private readonly defaultFrom: string;

  /**
   * Initializes the SendGrid SDK with the validated API key.
   *
   * @param configService - Provider for SendGrid credentials and sender default.
   * @returns An injectable delivery provider initialized for SendGrid.
   * @throws Propagates SDK initialization errors without logging credentials.
   */
  constructor(configService: EmailServiceConfigService) {
    const emailConfig = configService.email;
    this.defaultFrom = emailConfig.from;
    sendGridMail.setApiKey(emailConfig.sendGridApiKey);
  }

  /**
   * Maps a prepared payload to legacy-compatible SendGrid request fields.
   *
   * @param templateId - Resolved SendGrid template identifier.
   * @param payload - Normalized payload produced by EmailProcessingService.
   * @returns The SendGrid SDK result.
   * @remarks Initial and retry orchestration await this shared provider method.
   * @throws Propagates SendGrid delivery failures to the caller.
   */
  send(
    templateId: string,
    payload: NormalizedEmailPayload,
  ): ReturnType<typeof sendGridMail.send> {
    const request = this.buildRequest(templateId, payload);
    this.logger.log({
      event: 'email_delivery_started',
      templateId,
      recipientCount: payload.recipients.length,
      version: payload.version === 'v3' ? 'v3' : 'legacy',
    });
    return sendGridMail.send(request);
  }

  /**
   * Constructs the exact v3 or legacy SendGrid field mapping.
   *
   * @param templateId - Resolved template identifier.
   * @param payload - Normalized email payload to map.
   * @returns A request accepted by the SendGrid mail SDK.
   * @remarks Called by send immediately before invoking the SendGrid client.
   * @throws Never for normalized payloads; this method only maps fields.
   */
  private buildRequest(
    templateId: string,
    payload: NormalizedEmailPayload,
  ): MailDataRequired {
    const from = this.withDefault(payload.from);
    const replyTo = this.withDefault(payload.replyTo);
    const common = {
      to: payload.recipients,
      templateId,
      from,
      replyTo,
      categories: payload.categories ?? [],
      cc: payload.cc ?? [],
      bcc: payload.bcc ?? [],
    };

    if (payload.version === 'v3') {
      return {
        ...common,
        dynamicTemplateData: payload.data,
        attachments: payload.attachments ?? [],
        ...(payload.personalizations === undefined
          ? {}
          : { personalizations: payload.personalizations }),
        ...(payload.sendAt === undefined ? {} : { sendAt: payload.sendAt }),
      } as unknown as MailDataRequired;
    }

    return {
      ...common,
      substitutions: payload.data,
      substitutionWrappers: ['{{', '}}'],
    } as MailDataRequired;
  }

  /**
   * Applies the configured sender when an address is missing or empty.
   *
   * @param address - Optional caller-provided SendGrid address.
   * @returns The caller address or configured default sender.
   * @remarks Used by buildRequest for both from and replyTo values.
   * @throws Never; invalid address structures are rejected during normalization.
   */
  private withDefault(address: EmailRecipient | undefined): EmailRecipient {
    return typeof address === 'string' && address.trim().length === 0
      ? this.defaultFrom
      : (address ?? this.defaultFrom);
  }
}
