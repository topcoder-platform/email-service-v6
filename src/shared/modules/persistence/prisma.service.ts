import {
  BeforeApplicationShutdown,
  Injectable,
  OnModuleInit,
} from '@nestjs/common';
import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '@prisma/client';
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { LoggerService } from '../global/logger.service';

/**
 * Owns the email service Prisma client lifecycle and configures it with the
 * validated database URL and PostgreSQL schema. Disconnection is deferred
 * until every module-destroy hook has drained its persistence-dependent work.
 *
 * The service is registered by `EmailAttemptPersistenceModule` and shared with
 * persistence repositories through Nest dependency injection.
 */
@Injectable()
export class PrismaService
  extends PrismaClient
  implements OnModuleInit, BeforeApplicationShutdown
{
  private readonly logger: LoggerService;

  /**
   * Creates a Prisma client backed by the PostgreSQL driver adapter.
   *
   * @param configService - Validated email-service configuration provider.
   */
  constructor(configService: EmailServiceConfigService) {
    const database = configService.database;
    const adapter = new PrismaPg(
      { connectionString: database.url },
      { schema: database.schema },
    );

    super({ adapter });

    this.logger = LoggerService.forRoot('PrismaService');
    this.logger.log(`Using PostgreSQL schema: ${database.schema}`);
  }

  /**
   * Connects Prisma during Nest module initialization.
   *
   * @returns A promise that resolves after the database connection succeeds.
   * @throws Rethrows the Prisma connection error so application startup fails.
   */
  async onModuleInit(): Promise<void> {
    this.logger.log('Connecting to database');
    try {
      await this.$connect();
      this.logger.log('Database connection established');
    } catch (error) {
      this.logger.error('Failed to connect to database');
      throw error;
    }
  }

  /**
   * Disconnects Prisma immediately before Nest shuts down the application,
   * after retry and Kafka module-destroy hooks have settled.
   *
   * @returns A promise that resolves after the connection is closed.
   * @throws Propagates Prisma disconnection errors to application shutdown.
   */
  async beforeApplicationShutdown(): Promise<void> {
    await this.$disconnect();
  }
}
