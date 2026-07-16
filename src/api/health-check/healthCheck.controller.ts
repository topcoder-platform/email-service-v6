import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { LoggerService } from '../../shared/modules/global/logger.service';
import { KafkaConsumerService } from '../../shared/modules/kafka/kafka-consumer.service';
import { KafkaConnectionState } from '../../shared/modules/kafka/kafka.types';
import { PrismaService } from '../../shared/modules/persistence/prisma.service';

/** Overall health values returned by the health endpoint. */
export type HealthStatus = 'healthy' | 'unhealthy';

/** Database health values returned without raw connection details. */
export type DatabaseHealthStatus = 'connected' | 'unavailable';

/** Stable dependency-aware health response contract. */
export interface HealthCheckResponse {
  status: HealthStatus;
  database: { status: DatabaseHealthStatus };
  kafka: {
    state: KafkaConnectionState;
    reconnectAttempts: number;
  };
}

/**
 * Exposes the email service health endpoint using a fresh database probe and a
 * sanitized snapshot of the Kafka consumer lifecycle.
 */
@Controller('email')
export class HealthCheckController {
  private readonly logger = LoggerService.forRoot('HealthCheckController');

  /**
   * Creates the controller with required database and Kafka health providers.
   *
   * @param prisma - Prisma client used for a fresh query on every request.
   * @param kafkaConsumer - Kafka lifecycle owner providing immutable status.
   * @throws Never; construction only stores injected collaborators.
   */
  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaConsumer: KafkaConsumerService,
  ) {}

  /**
   * Reports dependency-aware health for load balancers and ECS.
   *
   * @returns A healthy response when the database is connected and Kafka has
   * not exhausted recovery.
   * @throws {ServiceUnavailableException} With the same sanitized response
   * schema when the database is unavailable or Kafka has failed permanently.
   */
  @Get('healthcheck')
  async healthCheck(): Promise<HealthCheckResponse> {
    let databaseStatus: DatabaseHealthStatus = 'connected';
    try {
      await this.prisma.$queryRaw`SELECT 1`;
    } catch {
      databaseStatus = 'unavailable';
      this.logger.error({ event: 'healthcheck_database_unavailable' });
    }

    const kafkaStatus = this.kafkaConsumer.getStatus();
    const healthy =
      databaseStatus === 'connected' &&
      kafkaStatus.state !== KafkaConnectionState.Failed;
    const response: HealthCheckResponse = {
      status: healthy ? 'healthy' : 'unhealthy',
      database: { status: databaseStatus },
      kafka: {
        state: kafkaStatus.state,
        reconnectAttempts: kafkaStatus.reconnectAttempts,
      },
    };

    if (!healthy) {
      this.logger.error({
        event: 'healthcheck_unhealthy',
        databaseStatus,
        kafkaState: kafkaStatus.state,
        reconnectAttempts: kafkaStatus.reconnectAttempts,
      });
      throw new ServiceUnavailableException(response);
    }

    return response;
  }
}
