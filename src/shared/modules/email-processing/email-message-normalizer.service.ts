import { Injectable } from '@nestjs/common';
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import {
  EmailAttachment,
  EmailNormalizationOutcome,
  EmailObject,
  EmailPersonalization,
  EmailPreparationSkipReason,
  EmailRecipient,
  EmailRecipientList,
  NormalizedEmailPayload,
} from './email-processing.types';

const COMMON_OPTIONAL_FIELDS = [
  'from',
  'replyTo',
  'data',
  'categories',
  'cc',
  'bcc',
  'version',
] as const;

const FALSEY_DEFAULT_OPTIONAL_FIELDS = [
  'from',
  'replyTo',
  'categories',
  'cc',
  'bcc',
] as const;

const V3_OPTIONAL_FIELDS = [
  'attachments',
  'personalizations',
  'sendAt',
] as const;

/**
 * Determines whether a value is a non-array JSON object.
 * Used by normalization and field validators before accessing keyed values.
 *
 * @param value - Value to inspect.
 * @returns True when the value can be treated as a keyed object.
 * @throws Never; the check does not access properties on unvalidated values.
 */
function isObject(value: unknown): value is EmailObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * Determines whether a value is a usable SendGrid address.
 * Used for required recipients and optional sender or copy fields.
 *
 * @param value - Candidate string or address object.
 * @param allowEmptyString - Whether an empty string represents an omitted value.
 * @returns True when SendGrid can use the address or delivery may default it.
 * @throws Never; malformed address values return false.
 */
function isAddress(
  value: unknown,
  allowEmptyString = false,
): value is EmailRecipient {
  if (typeof value === 'string') {
    return allowEmptyString || value.trim().length > 0;
  }
  if (!isObject(value) || typeof value.email !== 'string') {
    return false;
  }
  if (value.email.trim().length === 0) {
    return false;
  }
  return value.name === undefined || typeof value.name === 'string';
}

/**
 * Determines whether a value is an array of usable SendGrid addresses.
 * Used to validate recipient, carbon-copy, and blind-carbon-copy collections.
 *
 * @param value - Candidate recipient collection.
 * @param requireNonEmpty - Whether at least one recipient is required.
 * @returns True when every array item is a usable address.
 * @throws Never; malformed collections return false.
 */
function isAddressArray(
  value: unknown,
  requireNonEmpty: boolean,
): value is EmailRecipient[] {
  return (
    Array.isArray(value) &&
    (!requireNonEmpty || value.length > 0) &&
    value.every((recipient) => isAddress(recipient))
  );
}

/**
 * Determines whether a value is a usable singular or array recipient field.
 * Used for SendGrid fields that accept either representation.
 *
 * @param value - Candidate recipient or recipient collection.
 * @param requireNonEmpty - Whether an array must contain at least one address.
 * @returns True when the value contains only usable SendGrid addresses.
 * @throws Never; malformed recipient values return false.
 */
function isAddressList(
  value: unknown,
  requireNonEmpty: boolean,
): value is EmailRecipientList {
  return isAddress(value) || isAddressArray(value, requireNonEmpty);
}

/**
 * Determines whether a value is a SendGrid category or category collection.
 * The helper accepts a single string and normalizes it internally.
 *
 * @param value - Candidate category or category collection.
 * @returns True when the value is a string or contains only strings.
 * @throws Never; malformed category values return false.
 */
function isCategoryList(value: unknown): value is string | string[] {
  return (
    typeof value === 'string' ||
    (Array.isArray(value) && value.every((item) => typeof item === 'string'))
  );
}

/**
 * Determines whether an optional value is a nonnegative integer send time.
 *
 * @param value - Candidate Unix send time.
 * @returns True when the value satisfies the SendGrid integer contract.
 * @throws Never; malformed values return false.
 */
function isSendAt(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0;
}

/**
 * Determines whether a value is a string-valued object.
 * Used for personalization headers, substitutions, and custom arguments.
 *
 * @param value - Candidate string record.
 * @returns True when the value is an object whose values are all strings.
 * @throws Never; malformed records return false.
 */
function isStringRecord(value: unknown): value is Record<string, string> {
  return (
    isObject(value) &&
    Object.values(value).every((item) => typeof item === 'string')
  );
}

/**
 * Determines whether a value satisfies SendGrid's attachment contract.
 * Required content and filename fields and all supported optional fields are
 * checked before delivery can be attempted.
 *
 * @param value - Candidate attachment.
 * @returns True when SendGrid can construct the attachment at runtime.
 * @throws Never; malformed attachment values return false.
 */
function isAttachment(value: unknown): value is EmailAttachment {
  return (
    isObject(value) &&
    typeof value.content === 'string' &&
    typeof value.filename === 'string' &&
    (value.type === undefined || typeof value.type === 'string') &&
    (value.disposition === undefined ||
      typeof value.disposition === 'string') &&
    (value.contentId === undefined || typeof value.contentId === 'string')
  );
}

/**
 * Determines whether a value is a deliverable SendGrid personalization.
 * A supplied personalization must own at least one recipient because SendGrid
 * ignores the message-level recipient field when personalizations are present.
 *
 * @param value - Candidate personalization.
 * @returns True when recipients and all supported nested fields are valid.
 * @throws Never; malformed personalization values return false.
 */
function isPersonalization(value: unknown): value is EmailPersonalization {
  return (
    isObject(value) &&
    isAddressList(value.to, true) &&
    (value.from === undefined || isAddress(value.from)) &&
    (value.cc === undefined || isAddressList(value.cc, false)) &&
    (value.bcc === undefined || isAddressList(value.bcc, false)) &&
    (value.subject === undefined || typeof value.subject === 'string') &&
    (value.headers === undefined || isStringRecord(value.headers)) &&
    (value.substitutions === undefined ||
      isStringRecord(value.substitutions)) &&
    (value.dynamicTemplateData === undefined ||
      isObject(value.dynamicTemplateData)) &&
    (value.customArgs === undefined || isStringRecord(value.customArgs)) &&
    (value.sendAt === undefined || isSendAt(value.sendAt))
  );
}

/**
 * Determines whether a supplied personalization collection is deliverable.
 *
 * @param value - Candidate personalization collection.
 * @returns True for a nonempty array of valid personalizations.
 * @throws Never; malformed collections return false.
 */
function isPersonalizationArray(
  value: unknown,
): value is EmailPersonalization[] {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.every((personalization) => isPersonalization(personalization))
  );
}

/**
 * Safely decodes and validates raw email messages without Kafka dependencies.
 * Invalid input is represented as a stable skip outcome rather than an error.
 * Future message orchestration uses this provider before template resolution.
 */
@Injectable()
export class EmailMessageNormalizer {
  /**
   * Creates a normalizer aware of the configured template override field.
   *
   * @param configService - Validated email-service configuration provider.
   * @returns An injectable normalizer used by EmailProcessingService.
   * @throws Never; configuration was validated during application startup.
   */
  constructor(private readonly configService: EmailServiceConfigService) {}

  /**
   * Normalizes JSON text, bus envelopes, or already-decoded email payloads.
   *
   * @param input - Untrusted message value supplied by a future orchestrator.
   * @returns A normalized payload or a stable reason to skip the message.
   * @remarks EmailProcessingService uses this as the first preparation step.
   * Falsey sender, copy, and category fields are omitted so delivery defaults
   * apply. Falsey v3-only fields are also omitted, while non-v3 messages drop
   * all v3-only fields without validating them.
   * @throws Never; JSON parsing and structural failures become skip outcomes.
   */
  normalize(input: unknown): EmailNormalizationOutcome {
    const decoded = this.decode(input);
    if (!decoded) {
      return {
        ready: false,
        reason: EmailPreparationSkipReason.MalformedInput,
      };
    }

    const payload = Object.prototype.hasOwnProperty.call(decoded, 'payload')
      ? decoded.payload
      : decoded;
    if (!isObject(payload)) {
      return {
        ready: false,
        reason: EmailPreparationSkipReason.MalformedInput,
      };
    }
    if (!isAddressArray(payload.recipients, true)) {
      return {
        ready: false,
        reason: EmailPreparationSkipReason.InvalidRecipients,
      };
    }
    if (!this.hasValidOptionalFields(payload)) {
      return {
        ready: false,
        reason: EmailPreparationSkipReason.MalformedInput,
      };
    }

    const normalized: NormalizedEmailPayload = {
      recipients: payload.recipients,
    };
    for (const field of COMMON_OPTIONAL_FIELDS) {
      if (!Object.prototype.hasOwnProperty.call(payload, field)) {
        continue;
      }
      if (
        FALSEY_DEFAULT_OPTIONAL_FIELDS.includes(
          field as (typeof FALSEY_DEFAULT_OPTIONAL_FIELDS)[number],
        ) &&
        !payload[field]
      ) {
        continue;
      }
      Object.assign(normalized, { [field]: payload[field] });
    }
    if (payload.version === 'v3') {
      for (const field of V3_OPTIONAL_FIELDS) {
        if (
          Object.prototype.hasOwnProperty.call(payload, field) &&
          payload[field]
        ) {
          Object.assign(normalized, { [field]: payload[field] });
        }
      }
    }
    const overrideKey = this.configService.email.templateOverrideKey;
    if (Object.prototype.hasOwnProperty.call(payload, overrideKey)) {
      normalized[overrideKey] = payload[overrideKey];
    }

    return { ready: true, payload: normalized };
  }

  /**
   * Parses text input and rejects decoded values that are not JSON objects.
   *
   * @param input - Raw message value to decode.
   * @returns A decoded object, or undefined for malformed input.
   * @remarks Called only by normalize before envelope or payload inspection.
   * @throws Never; JSON parse errors are contained locally.
   */
  private decode(input: unknown): EmailObject | undefined {
    if (typeof input !== 'string') {
      return isObject(input) ? input : undefined;
    }
    try {
      const decoded: unknown = JSON.parse(input);
      return isObject(decoded) ? decoded : undefined;
    } catch {
      return undefined;
    }
  }

  /**
   * Validates supplied optional fields before they can reach SendGrid.
   *
   * @param payload - Decoded email object containing optional fields.
   * @returns True when retained supported fields are structurally usable.
   * @remarks Called by normalize before copying fields to the normalized payload.
   * Falsey defaultable fields are treated as omitted. V3-only fields are
   * validated only for v3 messages and are otherwise ignored without error.
   * @throws Never; each validator handles untrusted values defensively.
   */
  private hasValidOptionalFields(payload: EmailObject): boolean {
    return (
      (!payload.from || isAddress(payload.from)) &&
      (!payload.replyTo || isAddress(payload.replyTo)) &&
      (payload.data === undefined || isObject(payload.data)) &&
      (!payload.categories || isCategoryList(payload.categories)) &&
      (!payload.cc || isAddressList(payload.cc, false)) &&
      (!payload.bcc || isAddressList(payload.bcc, false)) &&
      (payload.version === undefined || typeof payload.version === 'string') &&
      (payload.version !== 'v3' ||
        ((!payload.attachments ||
          (Array.isArray(payload.attachments) &&
            payload.attachments.every((attachment) =>
              isAttachment(attachment),
            ))) &&
          (!payload.personalizations ||
            isPersonalizationArray(payload.personalizations)) &&
          (!payload.sendAt || isSendAt(payload.sendAt))))
    );
  }
}
