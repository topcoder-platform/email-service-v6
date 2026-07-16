import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { ApiModule } from './api/api.module';
import { EmailServiceConfigModule } from './config/email-service-config.module';
import { EmailRetryModule } from './shared/modules/email-retry/email-retry.module';
import { KafkaModule } from './shared/modules/kafka/kafka.module';

/**
 * Composes validated configuration, dynamic scheduling, Kafka-driven initial
 * delivery, failed-attempt retries, persistence, and the operational API.
 */
@Module({
  imports: [
    EmailServiceConfigModule,
    ScheduleModule.forRoot(),
    KafkaModule,
    EmailRetryModule,
    ApiModule,
  ],
  controllers: [],
  providers: [],
})
export class AppModule {}
