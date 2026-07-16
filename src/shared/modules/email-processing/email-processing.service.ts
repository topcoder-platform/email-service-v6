import { Injectable } from '@nestjs/common';
import { LoggerService } from '../global/logger.service';
import { EmailMessageNormalizer } from './email-message-normalizer.service';
import { EmailPreparationOutcome } from './email-processing.types';
import { EmailTemplateResolver } from './email-template-resolver.service';

/**
 * Coordinates side-effect-free email normalization and template resolution for
 * future orchestration code, which calls prepare before persistence or delivery.
 */
@Injectable()
export class EmailProcessingService {
  private readonly logger = LoggerService.forRoot('EmailProcessingService');

  /**
   * Creates the preparation boundary from its internal processing providers.
   *
   * @param normalizer - Provider that safely normalizes untrusted messages.
   * @param templateResolver - Provider that selects configured templates.
   * @returns An injectable preparation boundary for future orchestration.
   * @throws Never; construction only stores injected providers.
   */
  constructor(
    private readonly normalizer: EmailMessageNormalizer,
    private readonly templateResolver: EmailTemplateResolver,
  ) {}

  /**
   * Prepares a message for delivery without persistence or provider calls.
   *
   * @param topic - Topic used for template-map resolution and safe logging.
   * @param input - Raw JSON text, bus envelope, or decoded payload.
   * @returns A ready delivery input or a log-only skip outcome.
   * @remarks Future orchestration uses ready outcomes for persistence and delivery.
   * @throws Never for malformed input; providers return skip outcomes.
   */
  prepare(topic: string, input: unknown): EmailPreparationOutcome {
    const normalization = this.normalizer.normalize(input);
    if (!normalization.ready) {
      this.logSkip(topic, normalization.reason);
      return normalization;
    }

    const resolution = this.templateResolver.resolveTemplateId(
      topic,
      normalization.payload,
    );
    if (!resolution.ready) {
      this.logSkip(topic, resolution.reason);
      return resolution;
    }

    return {
      ready: true,
      payload: normalization.payload,
      templateId: resolution.templateId,
    };
  }

  /**
   * Emits only topic and stable reason metadata for a skipped message.
   *
   * @param topic - Topic associated with the rejected message.
   * @param reason - Stable preparation skip reason.
   * @returns Nothing.
   * @remarks Called by prepare for normalization and template-resolution skips.
   * @throws Never under the application logger contract.
   */
  private logSkip(topic: string, reason: string): void {
    this.logger.warn({ event: 'email_preparation_skipped', topic, reason });
  }
}
