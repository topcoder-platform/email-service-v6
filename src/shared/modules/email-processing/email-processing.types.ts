/** A SendGrid-compatible email address with an optional display name. */
export interface EmailAddress {
  email: string;
  name?: string;
}

/** A recipient accepted by SendGrid. */
export type EmailRecipient = string | EmailAddress;

/** One or more recipients in the singular or array forms accepted by SendGrid. */
export type EmailRecipientList = EmailRecipient | EmailRecipient[];

/** JSON object used for template data and extensible SendGrid structures. */
export type EmailObject = Record<string, unknown>;

/** A validated attachment accepted by the SendGrid mail helper at runtime. */
export interface EmailAttachment {
  content: string;
  filename: string;
  type?: string;
  disposition?: string;
  contentId?: string;
}

/**
 * A validated SendGrid personalization with its own required recipients and
 * optional per-recipient delivery fields.
 */
export interface EmailPersonalization {
  to: EmailRecipientList;
  from?: EmailRecipient;
  cc?: EmailRecipientList;
  bcc?: EmailRecipientList;
  subject?: string;
  headers?: Record<string, string>;
  substitutions?: Record<string, string>;
  dynamicTemplateData?: EmailObject;
  customArgs?: Record<string, string>;
  sendAt?: number;
}

/**
 * Contains the validated email fields needed by template resolution and
 * delivery. The index signature permits the configured template override key.
 */
export interface NormalizedEmailPayload {
  recipients: EmailRecipient[];
  from?: EmailRecipient;
  replyTo?: EmailRecipient;
  data?: EmailObject;
  categories?: string | string[];
  cc?: EmailRecipientList;
  bcc?: EmailRecipientList;
  version?: string;
  attachments?: EmailAttachment[];
  personalizations?: EmailPersonalization[];
  sendAt?: number;
  [key: string]: unknown;
}

/** Stable reasons a message can be skipped before delivery. */
export enum EmailPreparationSkipReason {
  MalformedInput = 'malformed_input',
  InvalidRecipients = 'invalid_recipients',
  InvalidOverrideValue = 'invalid_override_value',
  UnresolvedTemplate = 'unresolved_template',
}

/** A normalized message that can proceed to template resolution. */
export interface EmailNormalizationReady {
  ready: true;
  payload: NormalizedEmailPayload;
}

/** A message rejected during preparation without throwing. */
export interface EmailPreparationSkip {
  ready: false;
  reason: EmailPreparationSkipReason;
}

/** Result returned by message normalization. */
export type EmailNormalizationOutcome =
  EmailNormalizationReady | EmailPreparationSkip;

/** A prepared message ready for SendGrid delivery. */
export interface EmailPreparationReady {
  ready: true;
  payload: NormalizedEmailPayload;
  templateId: string;
}

/** Result returned by the side-effect-free preparation boundary. */
export type EmailPreparationOutcome =
  EmailPreparationReady | EmailPreparationSkip;

/** A resolved template identifier. */
export interface EmailTemplateReady {
  ready: true;
  templateId: string;
}

/** Result returned by template resolution. */
export type EmailTemplateResolutionOutcome =
  EmailTemplateReady | EmailPreparationSkip;
