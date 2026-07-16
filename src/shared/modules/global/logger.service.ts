import {
  Injectable,
  LoggerService as NestLoggerService,
  LogLevel,
} from '@nestjs/common';

const SENSITIVE_KEY_PATTERN =
  /api[_-]?key|private[_-]?key|cert|passphrase|password|secret|database[_-]?url|config/i;
const PEM_PATTERN = /-----BEGIN [A-Z ]+-----/;
const SENDGRID_KEY_PATTERN = /\bSG\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/;
const CONFIG_OBJECT_KEYS = new Set([
  'DATABASE_URL',
  'SENDGRID_API_KEY',
  'KAFKA_CLIENT_CERT',
  'KAFKA_CLIENT_CERT_KEY',
  'KAFKA_CA_CERT',
  'KAFKA_CLIENT_CERT_PASSPHRASE',
]);

/**
 * Provides context-aware application logging with defensive redaction for
 * credentials, certificates, keys, passphrases, and configuration objects.
 */
@Injectable()
export class LoggerService implements NestLoggerService {
  private context?: string;

  /**
   * Creates a logger for an optional application context.
   *
   * @param context - Label included in emitted log messages.
   */
  constructor(context?: string) {
    this.context = context;
  }

  /**
   * Creates a logger instance with a fixed context label.
   *
   * @param context - Label identifying the logging component.
   * @returns A new context-aware logger.
   */
  static forRoot(context: string): LoggerService {
    return new LoggerService(context);
  }

  /**
   * Updates the context label for subsequent messages.
   *
   * @param context - New logging context.
   * @returns Nothing.
   */
  setContext(context: string): void {
    this.context = context;
  }

  /**
   * Emits a standard informational message.
   *
   * @param message - Message or structured metadata to log safely.
   * @param context - Optional per-message context override.
   * @returns Nothing.
   */
  log(message: unknown, context?: string): void {
    this.printMessage('log', message, context ?? this.context);
  }

  /**
   * Emits an error message and an optional stack trace.
   *
   * @param message - Error description or structured metadata to log safely.
   * @param trace - Optional stack trace; PEM and SendGrid key content is redacted.
   * @param context - Optional per-message context override.
   * @returns Nothing.
   */
  error(message: unknown, trace?: string, context?: string): void {
    this.printMessage('error', message, context ?? this.context);
    if (trace) {
      console.error(this.sanitizeString(trace));
    }
  }

  /**
   * Emits a warning message.
   *
   * @param message - Warning description or structured metadata to log safely.
   * @param context - Optional per-message context override.
   * @returns Nothing.
   */
  warn(message: unknown, context?: string): void {
    this.printMessage('warn', message, context ?? this.context);
  }

  /**
   * Emits a debugging message.
   *
   * @param message - Debug description or structured metadata to log safely.
   * @param context - Optional per-message context override.
   * @returns Nothing.
   */
  debug(message: unknown, context?: string): void {
    this.printMessage('debug', message, context ?? this.context);
  }

  /**
   * Emits a verbose diagnostic message.
   *
   * @param message - Diagnostic description or structured metadata to log safely.
   * @param context - Optional per-message context override.
   * @returns Nothing.
   */
  verbose(message: unknown, context?: string): void {
    this.printMessage('verbose', message, context ?? this.context);
  }

  /**
   * Formats and routes a sanitized message to the matching console method.
   *
   * @param level - Nest log level for the message.
   * @param message - Value to sanitize and format.
   * @param context - Optional logging context.
   * @returns Nothing.
   */
  private printMessage(
    level: LogLevel,
    message: unknown,
    context?: string,
  ): void {
    const timestamp = new Date().toISOString();
    const sanitizedMessage = this.sanitizeValue(message);
    const logMessage =
      typeof sanitizedMessage === 'string'
        ? sanitizedMessage
        : JSON.stringify(sanitizedMessage);
    const formattedMessage = `[${timestamp}] [${level.toUpperCase()}] ${context ? `[${context}] ` : ''}${logMessage}`;

    switch (level) {
      case 'error':
        console.error(formattedMessage);
        break;
      case 'warn':
        console.warn(formattedMessage);
        break;
      case 'debug':
        console.debug(formattedMessage);
        break;
      default:
        console.log(formattedMessage);
    }
  }

  /**
   * Recursively redacts sensitive fields before structured values are logged.
   *
   * @param value - Arbitrary log value.
   * @returns A safe value suitable for serialization.
   */
  private sanitizeValue(value: unknown): unknown {
    if (typeof value === 'string') {
      return this.sanitizeString(value);
    }
    if (value instanceof Error) {
      return {
        name: value.name,
        message: this.sanitizeString(value.message),
      };
    }
    if (Array.isArray(value)) {
      return value.map((entry) => this.sanitizeValue(entry));
    }
    if (value !== null && typeof value === 'object') {
      const entries = Object.entries(value);
      if (entries.some(([key]) => CONFIG_OBJECT_KEYS.has(key))) {
        return '[REDACTED CONFIG]';
      }
      return Object.fromEntries(
        entries.map(([key, entry]) => [
          key,
          SENSITIVE_KEY_PATTERN.test(key)
            ? '[REDACTED]'
            : this.sanitizeValue(entry),
        ]),
      );
    }
    return value;
  }

  /**
   * Redacts recognizable PEM blocks and SendGrid API key shapes from text.
   *
   * @param value - Text being prepared for logging.
   * @returns Sanitized text.
   */
  private sanitizeString(value: string): string {
    if (PEM_PATTERN.test(value)) {
      return '[REDACTED PEM]';
    }
    return value.replace(SENDGRID_KEY_PATTERN, '[REDACTED SENDGRID KEY]');
  }
}
