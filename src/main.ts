import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from './app.module';
import { EmailServiceConfigService } from './config/email-service-config.service';
import { LoggerService } from './shared/modules/global/logger.service';

export const API_PREFIX = '/v6';

/**
 * Creates and starts the Nest application with validated configuration,
 * request parsing, Kafka lifecycle handling, CORS, and scheduling.
 * Failures after application creation close initialized lifecycle resources
 * before the original startup error is rethrown.
 *
 * @returns A promise that resolves after the HTTP listener starts.
 * @throws {Error} When configuration validation or application startup fails.
 */
export async function bootstrap(): Promise<void> {
  const logger = LoggerService.forRoot('Bootstrap');
  logger.log('Validating email service configuration');

  let app: NestExpressApplication;
  try {
    app = await NestFactory.create<NestExpressApplication>(AppModule, {
      rawBody: true,
      logger: ['error', 'warn', 'log', 'debug', 'verbose'],
    });
    logger.log('Email service configuration validated');
  } catch (error) {
    logger.error({ event: 'application_creation_failed' });
    throw error;
  }

  try {
    if (process.env.NODE_ENV === 'production') {
      app.setGlobalPrefix(API_PREFIX);
      logger.log(`Set production global prefix to ${API_PREFIX}`);
    }

    app.enableCors({
      allowedHeaders:
        'Origin, X-Requested-With, Content-Type, Accept, Authorization',
      credentials: true,
      origin: true,
      methods: 'GET, POST, OPTIONS, PUT, DELETE, PATCH',
    });
    app.useBodyParser('json', { limit: '15mb' });
    app.useBodyParser('urlencoded', { limit: '15mb', extended: true });
    app.useGlobalPipes(
      new ValidationPipe({ whitelist: true, transform: true }),
    );
    app.enableShutdownHooks();
    logger.log('CORS, body parsers, and validation pipe configured');

    const config = app.get(EmailServiceConfigService);
    await app.listen(config.app.port);
    logger.log(`Email service started on port ${config.app.port}`);
  } catch (error) {
    logger.error({ event: 'application_startup_failed' });
    try {
      await app.close();
    } catch {
      logger.error({ event: 'application_startup_cleanup_failed' });
    }
    throw error;
  }
}

/**
 * Runs application bootstrap and records a non-zero process outcome on failure.
 *
 * Bootstrap is responsible for closing any application created before a
 * startup failure, allowing this boundary to report only a stable safe event.
 *
 * @returns A promise that resolves after startup succeeds or failure is recorded.
 * @throws Never; bootstrap failures are converted to a non-zero process exit code.
 */
export async function startApplication(): Promise<void> {
  try {
    await bootstrap();
  } catch {
    const bootstrapLogger = LoggerService.forRoot('Bootstrap');
    bootstrapLogger.error({ event: 'email_service_startup_failed' });
    process.exitCode = 1;
  }
}

if (require.main === module) {
  void startApplication();
}
