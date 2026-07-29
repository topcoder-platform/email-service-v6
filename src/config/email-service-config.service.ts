import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EmailServiceEnv } from './email-service-env';

export interface AppConfig {
  port: number;
}

export interface DatabaseConfig {
  url: string;
  schema: string;
}

export interface EmailConfig {
  sendGridApiKey: string;
  from: string;
  templateMap: Record<string, string>;
  templateOverrideKey: string;
  retryCron: string;
  retryMaxAgeMs: number;
}

export interface KafkaConfig {
  brokers: string[];
  clientId?: string;
  groupId?: string;
  sslEnabled: boolean;
  clientCert?: string;
  clientCertKey?: string;
  caCert?: string;
  clientCertPassphrase?: string;
  connectionTimeout: number;
  requestTimeout: number;
  retryAttempts: number;
  initialRetryTime: number;
  maxRetryTime: number;
  maxBytes: number;
  maxWaitTime: number;
  disabled: boolean;
}

/**
 * Exposes validated email-service configuration as typed sections for
 * persistence, delivery, Kafka lifecycle, and retry consumers.
 */
@Injectable()
export class EmailServiceConfigService {
  /**
   * Creates typed access over the globally validated environment.
   *
   * @param configService - Nest configuration service populated at startup.
   */
  constructor(
    private readonly configService: ConfigService<EmailServiceEnv, true>,
  ) {}

  /** @returns Application listener configuration. */
  get app(): AppConfig {
    return { port: this.configService.get('PORT', { infer: true }) };
  }

  /** @returns Database connection settings for persistence providers. */
  get database(): DatabaseConfig {
    return {
      url: this.configService.get('DATABASE_URL', { infer: true }),
      schema: this.configService.get('POSTGRES_SCHEMA', { infer: true }),
    };
  }

  /** @returns SendGrid, template, and retry settings for email delivery. */
  get email(): EmailConfig {
    return {
      sendGridApiKey: this.configService.get('SENDGRID_API_KEY', {
        infer: true,
      }),
      from: this.configService.get('EMAIL_FROM', { infer: true }),
      templateMap: this.configService.get('EMAIL_TEMPLATE_MAP', {
        infer: true,
      }),
      templateOverrideKey: this.configService.get(
        'EMAIL_TEMPLATE_OVERRIDE_KEY',
        {
          infer: true,
        },
      ),
      retryCron: this.configService.get('EMAIL_RETRY_CRON', { infer: true }),
      retryMaxAgeMs: this.configService.get('EMAIL_RETRY_MAX_AGE_MS', {
        infer: true,
      }),
    };
  }

  /** @returns Kafka connection, fetch, and lifecycle retry settings. */
  get kafka(): KafkaConfig {
    return {
      brokers: this.configService.get('KAFKA_URL', { infer: true }),
      clientId: this.configService.get('KAFKA_CLIENT_ID', { infer: true }),
      groupId: this.configService.get('KAFKA_GROUP_ID', { infer: true }),
      sslEnabled: this.configService.get('KAFKA_SSL_ENABLED', { infer: true }),
      clientCert: this.configService.get('KAFKA_CLIENT_CERT', { infer: true }),
      clientCertKey: this.configService.get('KAFKA_CLIENT_CERT_KEY', {
        infer: true,
      }),
      caCert: this.configService.get('KAFKA_CA_CERT', { infer: true }),
      clientCertPassphrase: this.configService.get(
        'KAFKA_CLIENT_CERT_PASSPHRASE',
        { infer: true },
      ),
      connectionTimeout: this.configService.get('KAFKA_CONNECTION_TIMEOUT', {
        infer: true,
      }),
      requestTimeout: this.configService.get('KAFKA_REQUEST_TIMEOUT', {
        infer: true,
      }),
      retryAttempts: this.configService.get('KAFKA_RETRY_ATTEMPTS', {
        infer: true,
      }),
      initialRetryTime: this.configService.get('KAFKA_INITIAL_RETRY_TIME', {
        infer: true,
      }),
      maxRetryTime: this.configService.get('KAFKA_MAX_RETRY_TIME', {
        infer: true,
      }),
      maxBytes: this.configService.get('KAFKA_MAXBYTES', { infer: true }),
      maxWaitTime: this.configService.get('KAFKA_MAX_WAIT_TIME', {
        infer: true,
      }),
      disabled: this.configService.get('DISABLE_KAFKA', { infer: true }),
    };
  }
}
