import { Injectable } from '@nestjs/common';
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import {
  EmailPreparationSkipReason,
  EmailTemplateResolutionOutcome,
  NormalizedEmailPayload,
} from './email-processing.types';

/**
 * Resolves a validated template ID using payload override precedence.
 * EmailProcessingService uses this provider after successful normalization.
 */
@Injectable()
export class EmailTemplateResolver {
  /**
   * Creates a resolver backed by validated email configuration.
   *
   * @param configService - Provider for override-key and topic-map settings.
   * @returns An injectable resolver used by EmailProcessingService.
   * @throws Never; configuration was validated during application startup.
   */
  constructor(private readonly configService: EmailServiceConfigService) {}

  /**
   * Selects the payload override when present, otherwise the topic mapping.
   *
   * @param topic - Current message topic used for configured fallback lookup.
   * @param payload - Normalized email payload that may contain an override.
   * @returns A template ID or a stable skip reason.
   * @remarks Called during preparation after the payload has been normalized.
   * @throws Never; invalid overrides and mappings become skip outcomes.
   */
  resolveTemplateId(
    topic: string,
    payload: NormalizedEmailPayload,
  ): EmailTemplateResolutionOutcome {
    const { templateOverrideKey, templateMap } = this.configService.email;
    if (Object.prototype.hasOwnProperty.call(payload, templateOverrideKey)) {
      const override = payload[templateOverrideKey];
      if (typeof override !== 'string' || override.trim().length === 0) {
        return {
          ready: false,
          reason: EmailPreparationSkipReason.InvalidOverrideValue,
        };
      }
      return { ready: true, templateId: override };
    }

    const templateId = templateMap[topic];
    if (typeof templateId !== 'string' || templateId.trim().length === 0) {
      return {
        ready: false,
        reason: EmailPreparationSkipReason.UnresolvedTemplate,
      };
    }
    return { ready: true, templateId };
  }
}
