import { Module } from '@nestjs/common';
import { KafkaModule } from '../shared/modules/kafka/kafka.module';
import { EmailAttemptPersistenceModule } from '../shared/modules/persistence/email-attempt-persistence.module';
import { HealthCheckController } from './health-check/healthCheck.controller';

/**
 * Registers HTTP controllers with the persistence and Kafka status providers
 * required to expose the dependency-aware health contract.
 */
@Module({
  imports: [EmailAttemptPersistenceModule, KafkaModule],
  controllers: [HealthCheckController],
})
export class ApiModule {}
