import { ServiceUnavailableException } from '@nestjs/common';
import type { KafkaConsumerService } from '../../shared/modules/kafka/kafka-consumer.service';
import {
  KafkaConnectionState,
  type KafkaConsumerStatus,
} from '../../shared/modules/kafka/kafka.types';
import type { PrismaService } from '../../shared/modules/persistence/prisma.service';
import {
  HealthCheckController,
  type HealthCheckResponse,
} from './healthCheck.controller';

interface HealthInvocationResult {
  httpStatus: number;
  body: HealthCheckResponse;
}

/**
 * Invokes the controller and converts Nest service-unavailable exceptions into
 * an HTTP-like status and body for focused unit assertions.
 *
 * @param controller - Health controller under test.
 * @returns The effective HTTP status and sanitized health response.
 * @throws Rethrows unexpected exception types from the controller.
 */
async function invokeHealthCheck(
  controller: HealthCheckController,
): Promise<HealthInvocationResult> {
  try {
    return { httpStatus: 200, body: await controller.healthCheck() };
  } catch (error) {
    if (!(error instanceof ServiceUnavailableException)) {
      throw error;
    }
    return {
      httpStatus: error.getStatus(),
      body: error.getResponse() as HealthCheckResponse,
    };
  }
}

describe('HealthCheckController', () => {
  const queryRaw = jest.fn();
  const prisma = { $queryRaw: queryRaw } as unknown as PrismaService;
  const getStatus = jest.fn<KafkaConsumerStatus, []>();
  const kafkaConsumer = { getStatus } as unknown as KafkaConsumerService;
  let controller: HealthCheckController;

  beforeEach(() => {
    jest.clearAllMocks();
    queryRaw.mockResolvedValue([{ result: 1 }]);
    getStatus.mockReturnValue({
      state: KafkaConnectionState.Ready,
      reconnectAttempts: 0,
    });
    controller = new HealthCheckController(prisma, kafkaConsumer);
  });

  it.each([
    KafkaConnectionState.Initializing,
    KafkaConnectionState.Ready,
    KafkaConnectionState.Reconnecting,
    KafkaConnectionState.Disabled,
  ])('returns 200 for connected database and Kafka state %s', async (state) => {
    getStatus.mockReturnValue({ state, reconnectAttempts: 3 });

    const result = await invokeHealthCheck(controller);

    expect(result).toEqual({
      httpStatus: 200,
      body: {
        status: 'healthy',
        database: { status: 'connected' },
        kafka: { state, reconnectAttempts: 3 },
      },
    });
  });

  it('returns 503 for connected database after Kafka recovery is exhausted', async () => {
    getStatus.mockReturnValue({
      state: KafkaConnectionState.Failed,
      reconnectAttempts: 5,
      failureReason: 'broker address and certificate details',
    });

    const result = await invokeHealthCheck(controller);

    expect(result).toEqual({
      httpStatus: 503,
      body: {
        status: 'unhealthy',
        database: { status: 'connected' },
        kafka: {
          state: KafkaConnectionState.Failed,
          reconnectAttempts: 5,
        },
      },
    });
  });

  it.each(Object.values(KafkaConnectionState))(
    'returns 503 for unavailable database and Kafka state %s',
    async (state) => {
      queryRaw.mockRejectedValue(
        new Error('postgresql://secret-user:secret-password@private-host'),
      );
      getStatus.mockReturnValue({
        state,
        reconnectAttempts: 7,
        failureReason: 'secret Kafka dependency failure',
      });

      const result = await invokeHealthCheck(controller);

      expect(result).toEqual({
        httpStatus: 503,
        body: {
          status: 'unhealthy',
          database: { status: 'unavailable' },
          kafka: { state, reconnectAttempts: 7 },
        },
      });
    },
  );

  it('executes a fresh database query on consecutive requests', async () => {
    await invokeHealthCheck(controller);
    await invokeHealthCheck(controller);

    expect(queryRaw).toHaveBeenCalledTimes(2);
  });

  it('omits dependency errors and Kafka failure reasons from responses and logs', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation();
    queryRaw.mockRejectedValue(
      new Error('database-exception-secret private-database-host'),
    );
    getStatus.mockReturnValue({
      state: KafkaConnectionState.Failed,
      reconnectAttempts: 9,
      failureReason: 'kafka-failure-reason-secret private-broker-host',
    });

    const result = await invokeHealthCheck(controller);
    const serializedResponse = JSON.stringify(result.body);
    const loggedOutput = errorSpy.mock.calls.flat().join(' ');

    expect(serializedResponse).not.toContain('database-exception-secret');
    expect(serializedResponse).not.toContain('kafka-failure-reason-secret');
    expect(loggedOutput).not.toContain('database-exception-secret');
    expect(loggedOutput).not.toContain('kafka-failure-reason-secret');
    expect(loggedOutput).not.toContain('private-database-host');
    expect(loggedOutput).not.toContain('private-broker-host');

    errorSpy.mockRestore();
  });
});
