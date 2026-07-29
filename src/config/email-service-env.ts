import { X509Certificate } from 'node:crypto';
import { createSecureContext } from 'node:tls';
import { plainToInstance } from 'class-transformer';
import {
  IsArray,
  IsBoolean,
  IsEmail,
  IsInt,
  IsNotEmpty,
  IsObject,
  IsOptional,
  IsString,
  Min,
  validateSync,
  ValidateIf,
} from 'class-validator';
import {
  normalizePemNewlines,
  parseBoolean,
  parseCommaSeparatedList,
  parseJsonObject,
  parsePositiveInteger,
} from './env-parsers';

/** Default six-field retry schedule: every two minutes at second zero. */
export const DEFAULT_EMAIL_RETRY_CRON = '0 */2 * * * *';

/** Default retry eligibility window in milliseconds: 24 hours. */
export const DEFAULT_EMAIL_RETRY_MAX_AGE_MS = 86_400_000;

/** Stable safe error returned for every active Kafka mTLS validation failure. */
export const KAFKA_MTLS_VALIDATION_ERROR = 'Invalid Kafka mTLS configuration';

/**
 * Represents the validated and normalized email-service environment used by
 * persistence, delivery, Kafka lifecycle, and future retry components.
 */
export class EmailServiceEnv {
  @IsInt()
  @Min(1)
  PORT!: number;

  @IsString()
  @IsNotEmpty()
  DATABASE_URL!: string;

  @IsString()
  @IsNotEmpty()
  POSTGRES_SCHEMA!: string;

  @IsString()
  @IsNotEmpty()
  SENDGRID_API_KEY!: string;

  @IsEmail()
  EMAIL_FROM!: string;

  @IsObject()
  EMAIL_TEMPLATE_MAP!: Record<string, string>;

  @IsString()
  @IsNotEmpty()
  EMAIL_TEMPLATE_OVERRIDE_KEY!: string;

  @IsString()
  @IsNotEmpty()
  EMAIL_RETRY_CRON!: string;

  @IsInt()
  @Min(1)
  EMAIL_RETRY_MAX_AGE_MS!: number;

  @ValidateIf((environment: EmailServiceEnv) => !environment.DISABLE_KAFKA)
  @IsArray()
  @IsString({ each: true })
  @IsNotEmpty({ each: true })
  KAFKA_URL!: string[];

  @ValidateIf((environment: EmailServiceEnv) => !environment.DISABLE_KAFKA)
  @IsString()
  @IsNotEmpty()
  KAFKA_CLIENT_ID?: string;

  @ValidateIf((environment: EmailServiceEnv) => !environment.DISABLE_KAFKA)
  @IsString()
  @IsNotEmpty()
  KAFKA_GROUP_ID?: string;

  @IsBoolean()
  KAFKA_SSL_ENABLED!: boolean;

  @ValidateIf(
    (environment: EmailServiceEnv) =>
      !environment.DISABLE_KAFKA && environment.KAFKA_SSL_ENABLED,
  )
  @IsString()
  @IsNotEmpty()
  KAFKA_CLIENT_CERT?: string;

  @ValidateIf(
    (environment: EmailServiceEnv) =>
      !environment.DISABLE_KAFKA && environment.KAFKA_SSL_ENABLED,
  )
  @IsString()
  @IsNotEmpty()
  KAFKA_CLIENT_CERT_KEY?: string;

  @IsOptional()
  @IsString()
  KAFKA_CA_CERT?: string;

  @IsOptional()
  @IsString()
  KAFKA_CLIENT_CERT_PASSPHRASE?: string;

  @IsInt()
  @Min(1)
  KAFKA_CONNECTION_TIMEOUT!: number;

  @IsInt()
  @Min(1)
  KAFKA_REQUEST_TIMEOUT!: number;

  @IsInt()
  @Min(1)
  KAFKA_RETRY_ATTEMPTS!: number;

  @IsInt()
  @Min(1)
  KAFKA_INITIAL_RETRY_TIME!: number;

  @IsInt()
  @Min(1)
  KAFKA_MAX_RETRY_TIME!: number;

  @IsInt()
  @Min(1)
  KAFKA_MAXBYTES!: number;

  @IsInt()
  @Min(1)
  KAFKA_MAX_WAIT_TIME!: number;

  @IsBoolean()
  DISABLE_KAFKA!: boolean;
}

/**
 * Reads a required non-empty string without including its value in errors.
 *
 * @param environment - Raw environment object.
 * @param name - Required environment variable name.
 * @returns The trimmed environment value.
 * @throws {Error} When the variable is absent or empty.
 */
function requireString(
  environment: Record<string, unknown>,
  name: string,
): string {
  const value = environment[name];
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`${name} is required`);
  }
  return value.trim();
}

/**
 * Reads an optional non-empty string without including its value in errors.
 *
 * @param environment - Raw environment object.
 * @param name - Optional environment variable name.
 * @returns The trimmed value, or `undefined` when omitted.
 */
function optionalString(
  environment: Record<string, unknown>,
  name: string,
): string | undefined {
  const value = environment[name];
  if (value === undefined || value === '') {
    return undefined;
  }
  if (typeof value !== 'string') {
    throw new Error(`${name} must be a string`);
  }
  return value.trim();
}

/**
 * Reads an optional secret string without altering significant whitespace.
 * An absent or exactly empty value is treated as omitted, while every other
 * string, including a whitespace-only value, is returned exactly as supplied.
 * This is used for cryptographic secrets whose whitespace is significant.
 *
 * @param environment - Raw environment object.
 * @param name - Optional secret environment variable name.
 * @returns The exact supplied string, or `undefined` when absent or empty.
 * @throws {Error} When a supplied value is not a string.
 */
function optionalSecretString(
  environment: Record<string, unknown>,
  name: string,
): string | undefined {
  const value = environment[name];
  if (value === undefined || value === '') {
    return undefined;
  }
  if (typeof value !== 'string') {
    throw new Error(`${name} must be a string`);
  }
  return value;
}

/**
 * Verifies that a supplied CA bundle consists only of complete, individually
 * valid PEM certificate blocks separated by whitespace. This prevents a valid
 * first certificate from hiding malformed trailing bundle content during
 * active Kafka mTLS startup validation.
 *
 * @param caBundle - Normalized PEM CA bundle supplied for Kafka mTLS.
 * @returns Nothing after every certificate in the bundle has been validated.
 * @throws {Error} When the bundle is empty, contains unexpected content or
 * incomplete delimiters, or includes a malformed certificate.
 */
function validateCaCertificateBundle(caBundle: string): void {
  const certificateBlockPattern =
    /-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g;
  let consumedLength = 0;
  let certificateCount = 0;

  for (const match of caBundle.matchAll(certificateBlockPattern)) {
    const startIndex = match.index;
    if (caBundle.slice(consumedLength, startIndex).trim() !== '') {
      throw new Error(KAFKA_MTLS_VALIDATION_ERROR);
    }

    new X509Certificate(match[0]);
    certificateCount += 1;
    consumedLength = startIndex + match[0].length;
  }

  if (certificateCount === 0 || caBundle.slice(consumedLength).trim() !== '') {
    throw new Error(KAFKA_MTLS_VALIDATION_ERROR);
  }
}

/**
 * Validates the topic-to-SendGrid-template mapping used for Kafka delivery.
 *
 * @param value - Raw JSON environment value.
 * @returns A mapping containing at least one non-empty topic and template ID.
 * @throws {Error} When the JSON object contains invalid or empty entries.
 */
function parseEmailTemplateMap(value: unknown): Record<string, string> {
  const parsedMap = parseJsonObject(value, 'EMAIL_TEMPLATE_MAP');
  const entries = Object.entries(parsedMap);

  if (entries.length === 0) {
    throw new Error(
      'EMAIL_TEMPLATE_MAP must contain non-empty topic and template ID strings',
    );
  }

  const templateMap: Record<string, string> = {};
  for (const [topic, templateId] of entries) {
    if (
      topic.trim() === '' ||
      typeof templateId !== 'string' ||
      templateId.trim() === ''
    ) {
      throw new Error(
        'EMAIL_TEMPLATE_MAP must contain non-empty topic and template ID strings',
      );
    }
    templateMap[topic.trim()] = templateId.trim();
  }

  return templateMap;
}

/**
 * Converts raw process environment values into validated service config and
 * locally verifies normalized Kafka cert, key, optional CA, and passphrase
 * compatibility without contacting a broker. Cryptographic validation is
 * skipped when Kafka or Kafka SSL is disabled; broker connectivity remains the
 * responsibility of the non-blocking Kafka lifecycle.
 *
 * @param environment - Raw configuration supplied by `@nestjs/config`.
 * @returns Normalized environment values safe for typed config access.
 * @throws {Error} When required values are absent or structurally invalid, or
 * the stable safe mTLS error when active cryptographic material is unusable.
 */
export function validateEmailServiceEnv(
  environment: Record<string, unknown>,
): EmailServiceEnv {
  const disableKafka = parseBoolean(
    environment.DISABLE_KAFKA,
    'DISABLE_KAFKA',
    false,
  );
  const kafkaSslEnabled = parseBoolean(
    environment.KAFKA_SSL_ENABLED,
    'KAFKA_SSL_ENABLED',
  );
  const kafkaCaCertificateWasSupplied = environment.KAFKA_CA_CERT !== undefined;
  const kafkaCaCertificate = normalizePemNewlines(environment.KAFKA_CA_CERT);

  const validatedEnvironment = plainToInstance(EmailServiceEnv, {
    PORT: parsePositiveInteger(environment.PORT, 'PORT', 3000),
    DATABASE_URL: requireString(environment, 'DATABASE_URL'),
    POSTGRES_SCHEMA: requireString(environment, 'POSTGRES_SCHEMA'),
    SENDGRID_API_KEY: requireString(environment, 'SENDGRID_API_KEY'),
    EMAIL_FROM: requireString(environment, 'EMAIL_FROM'),
    EMAIL_TEMPLATE_MAP: parseEmailTemplateMap(environment.EMAIL_TEMPLATE_MAP),
    EMAIL_TEMPLATE_OVERRIDE_KEY:
      optionalString(environment, 'EMAIL_TEMPLATE_OVERRIDE_KEY') ??
      'sendgrid_template_id',
    EMAIL_RETRY_CRON:
      optionalString(environment, 'EMAIL_RETRY_CRON') ??
      DEFAULT_EMAIL_RETRY_CRON,
    EMAIL_RETRY_MAX_AGE_MS: parsePositiveInteger(
      environment.EMAIL_RETRY_MAX_AGE_MS,
      'EMAIL_RETRY_MAX_AGE_MS',
      DEFAULT_EMAIL_RETRY_MAX_AGE_MS,
    ),
    KAFKA_URL: parseCommaSeparatedList(
      environment.KAFKA_URL,
      'KAFKA_URL',
      disableKafka,
    ),
    KAFKA_CLIENT_ID: disableKafka
      ? optionalString(environment, 'KAFKA_CLIENT_ID')
      : requireString(environment, 'KAFKA_CLIENT_ID'),
    KAFKA_GROUP_ID: disableKafka
      ? optionalString(environment, 'KAFKA_GROUP_ID')
      : requireString(environment, 'KAFKA_GROUP_ID'),
    KAFKA_SSL_ENABLED: kafkaSslEnabled,
    KAFKA_CLIENT_CERT: normalizePemNewlines(environment.KAFKA_CLIENT_CERT),
    KAFKA_CLIENT_CERT_KEY: normalizePemNewlines(
      environment.KAFKA_CLIENT_CERT_KEY,
    ),
    KAFKA_CA_CERT: kafkaCaCertificate,
    KAFKA_CLIENT_CERT_PASSPHRASE: optionalSecretString(
      environment,
      'KAFKA_CLIENT_CERT_PASSPHRASE',
    ),
    KAFKA_CONNECTION_TIMEOUT: parsePositiveInteger(
      environment.KAFKA_CONNECTION_TIMEOUT,
      'KAFKA_CONNECTION_TIMEOUT',
    ),
    KAFKA_REQUEST_TIMEOUT: parsePositiveInteger(
      environment.KAFKA_REQUEST_TIMEOUT,
      'KAFKA_REQUEST_TIMEOUT',
    ),
    KAFKA_RETRY_ATTEMPTS: parsePositiveInteger(
      environment.KAFKA_RETRY_ATTEMPTS,
      'KAFKA_RETRY_ATTEMPTS',
    ),
    KAFKA_INITIAL_RETRY_TIME: parsePositiveInteger(
      environment.KAFKA_INITIAL_RETRY_TIME,
      'KAFKA_INITIAL_RETRY_TIME',
    ),
    KAFKA_MAX_RETRY_TIME: parsePositiveInteger(
      environment.KAFKA_MAX_RETRY_TIME,
      'KAFKA_MAX_RETRY_TIME',
    ),
    KAFKA_MAXBYTES: parsePositiveInteger(
      environment.KAFKA_MAXBYTES,
      'KAFKA_MAXBYTES',
    ),
    KAFKA_MAX_WAIT_TIME: parsePositiveInteger(
      environment.KAFKA_MAX_WAIT_TIME,
      'KAFKA_MAX_WAIT_TIME',
    ),
    DISABLE_KAFKA: disableKafka,
  });

  const errors = validateSync(validatedEnvironment, {
    skipMissingProperties: false,
    whitelist: true,
    forbidNonWhitelisted: true,
  });
  if (errors.length > 0) {
    if (
      !disableKafka &&
      kafkaSslEnabled &&
      errors.some(
        (error) =>
          error.property === 'KAFKA_CLIENT_CERT' ||
          error.property === 'KAFKA_CLIENT_CERT_KEY',
      )
    ) {
      throw new Error(KAFKA_MTLS_VALIDATION_ERROR);
    }

    const messages = errors.flatMap((error) =>
      Object.values(
        error.constraints ?? { invalid: `${error.property} is invalid` },
      ),
    );
    throw new Error(
      `Invalid email service configuration: ${messages.join('; ')}`,
    );
  }

  if (!disableKafka && kafkaSslEnabled) {
    try {
      if (kafkaCaCertificateWasSupplied) {
        validateCaCertificateBundle(kafkaCaCertificate ?? '');
      }
      createSecureContext({
        cert: validatedEnvironment.KAFKA_CLIENT_CERT,
        key: validatedEnvironment.KAFKA_CLIENT_CERT_KEY,
        ...(validatedEnvironment.KAFKA_CA_CERT === undefined
          ? {}
          : { ca: validatedEnvironment.KAFKA_CA_CERT }),
        ...(validatedEnvironment.KAFKA_CLIENT_CERT_PASSPHRASE === undefined
          ? {}
          : {
              passphrase: validatedEnvironment.KAFKA_CLIENT_CERT_PASSPHRASE,
            }),
      });
    } catch {
      throw new Error(KAFKA_MTLS_VALIDATION_ERROR);
    }
  }

  return validatedEnvironment;
}
