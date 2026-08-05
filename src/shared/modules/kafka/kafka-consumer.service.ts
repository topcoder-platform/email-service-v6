import {
  Injectable,
  OnApplicationBootstrap,
  OnModuleDestroy,
} from '@nestjs/common';
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { LoggerService } from '../global/logger.service';
import { KafkaEmailOrchestratorService } from './kafka-email-orchestrator.service';
import {
  KafkaConnectionState,
  type KafkaConsumerClient,
  type KafkaConsumerStatus,
  type KafkaMessageStream,
} from './kafka.types';
import { PlatformaticKafkaClientFactory } from './platformatic-kafka-client.factory';

interface ConsumerListeners {
  generation: number;
  error: (error: unknown) => void;
}

interface StreamListeners {
  generation: number;
  error: (error: unknown) => void;
  autocommit: (error?: unknown) => void;
  end: () => void;
  close: () => void;
}

/**
 * Prevents a detached Kafka EventEmitter from escalating a late `error` event
 * when its asynchronous close has failed and the lifecycle can no longer own it.
 *
 * @returns Nothing.
 */
const ignoreDetachedKafkaError = (): void => undefined;

/**
 * Owns the non-blocking Kafka consumer lifecycle, sequential message handling,
 * committed-offset subscription, bounded reconnection, status reporting, and
 * graceful application shutdown across every active processing generation.
 */
@Injectable()
export class KafkaConsumerService
  implements OnApplicationBootstrap, OnModuleDestroy
{
  private readonly logger = LoggerService.forRoot('KafkaConsumerService');
  private readonly topics: string[];
  private status: KafkaConsumerStatus;
  private consumer?: KafkaConsumerClient;
  private stream?: KafkaMessageStream;
  private consumerListeners?: ConsumerListeners;
  private streamListeners?: StreamListeners;
  private resourceGeneration = 0;
  private shuttingDown = false;
  private startupPromise?: Promise<void>;
  private reconnectPromise?: Promise<void>;
  private reconnectRequested = false;
  private queuedReconnectFailureReason?: string;
  private readonly processingPromises = new Set<Promise<void>>();
  private cancelBackoff?: () => void;

  /**
   * Creates the lifecycle owner and derives subscriptions from every template.
   *
   * @param configService - Validated Kafka and email topic configuration.
   * @param clientFactory - Factory that creates isolated consumer generations.
   * @param orchestrator - Sequential per-message email orchestration boundary.
   * @throws Never; disabled state and topic subscriptions are set synchronously.
   */
  constructor(
    private readonly configService: EmailServiceConfigService,
    private readonly clientFactory: PlatformaticKafkaClientFactory,
    private readonly orchestrator: KafkaEmailOrchestratorService,
  ) {
    this.topics = Object.keys(configService.email.templateMap);
    this.status = Object.freeze({
      state: configService.kafka.disabled
        ? KafkaConnectionState.Disabled
        : KafkaConnectionState.Initializing,
      reconnectAttempts: 0,
    });
  }

  /**
   * Starts Kafka initialization without making Nest bootstrap await the broker.
   *
   * @returns Nothing so an unavailable broker cannot prevent HTTP listening.
   * @throws Never; initialization failures enter the internal reconnect loop.
   */
  onApplicationBootstrap(): void {
    if (this.status.state === KafkaConnectionState.Disabled) {
      return;
    }

    const startupPromise = this.establishConnection()
      .catch((error: unknown) => {
        if (!this.shuttingDown) {
          this.requestReconnect(error);
        }
      })
      .finally(() => {
        if (this.startupPromise === startupPromise) {
          this.startupPromise = undefined;
        }
      });
    this.startupPromise = startupPromise;
  }

  /**
   * Stops backoff immediately and closes all Kafka resources during shutdown.
   *
   * @returns A promise resolved after lifecycle, resources, and all processing settle.
   * @throws Never; cleanup errors are safely logged and contained.
   */
  async onModuleDestroy(): Promise<void> {
    this.shuttingDown = true;
    this.cancelBackoff?.();

    await this.cleanupResources();
    await Promise.allSettled(
      [this.startupPromise, this.reconnectPromise].filter(
        (promise): promise is Promise<void> => promise !== undefined,
      ),
    );
    await this.cleanupResources();
    await Promise.allSettled([...this.processingPromises]);
  }

  /**
   * Returns a detached immutable snapshot for health reporting.
   *
   * @returns Current state, reconnect-attempt count, and optional safe reason.
   * @throws Never; the method only copies internal primitive status fields.
   */
  getStatus(): KafkaConsumerStatus {
    return Object.freeze({ ...this.status });
  }

  /**
   * Creates, subscribes, and starts processing one fresh resource generation.
   *
   * @returns A promise resolved once the consumer stream is ready.
   * @throws Propagates package, client-construction, and subscription failures.
   */
  private async establishConnection(): Promise<void> {
    const generation = ++this.resourceGeneration;
    const consumer = await this.clientFactory.createConsumer();
    if (this.shuttingDown || generation !== this.resourceGeneration) {
      await this.closeSafely(consumer, 'consumer');
      return;
    }

    this.consumer = consumer;
    this.attachConsumerListeners(consumer, generation);
    const kafka = this.configService.kafka;
    const stream = await consumer.consume({
      topics: [...this.topics],
      autocommit: true,
      mode: 'committed',
      fallbackMode: 'latest',
      maxBytes: kafka.maxBytes,
      maxWaitTime: kafka.maxWaitTime,
    });
    if (this.shuttingDown || generation !== this.resourceGeneration) {
      await this.closeSafely(stream, 'stream');
      await this.closeSafely(consumer, 'consumer');
      return;
    }

    this.stream = stream;
    this.attachStreamListeners(stream, generation);
    const queuedFailureReason = this.takeQueuedReconnectFailureReason();
    if (queuedFailureReason !== undefined) {
      throw new Error(queuedFailureReason);
    }
    this.setStatus(KafkaConnectionState.Ready, 0);
    this.startSequentialProcessing(stream, generation);
  }

  /**
   * Attaches whole-client failure listeners scoped to one resource generation.
   *
   * @param consumer - Active consumer that emits top-level client failures.
   * @param generation - Identity used to suppress stale failure signals.
   * @returns Nothing.
   * @throws Never; EventEmitter listener registration is synchronous.
   */
  private attachConsumerListeners(
    consumer: KafkaConsumerClient,
    generation: number,
  ): void {
    const listeners: ConsumerListeners = {
      generation,
      error: (error: unknown) => this.requestReconnect(error, generation),
    };
    consumer.on('error', listeners.error);
    this.consumerListeners = listeners;
  }

  /**
   * Attaches stream failure and abnormal-termination listeners for a generation.
   *
   * @param stream - Active subscribed Kafka message stream.
   * @param generation - Identity used to suppress stale failure signals.
   * @returns Nothing.
   * @throws Never; EventEmitter listener registration is synchronous.
   */
  private attachStreamListeners(
    stream: KafkaMessageStream,
    generation: number,
  ): void {
    const listeners: StreamListeners = {
      generation,
      error: (error: unknown) => this.requestReconnect(error, generation),
      autocommit: (error?: unknown) => {
        if (error !== undefined && error !== null) {
          this.requestReconnect(error, generation);
        }
      },
      end: () =>
        this.requestReconnect('Kafka message stream ended', generation),
      close: () =>
        this.requestReconnect('Kafka message stream closed', generation),
    };
    stream.on('error', listeners.error);
    stream.on('autocommit', listeners.autocommit);
    stream.on('end', listeners.end);
    stream.on('close', listeners.close);
    this.streamListeners = listeners;
  }

  /**
   * Iterates a stream serially and awaits orchestration before reading onward.
   *
   * @param stream - Active async Kafka message stream.
   * @param generation - Identity used to ignore intentional or stale closure.
   * @returns Nothing; every active processing promise is retained for shutdown.
   * @throws Never; iterator failures are routed to bounded reconnection.
   */
  private startSequentialProcessing(
    stream: KafkaMessageStream,
    generation: number,
  ): void {
    const processingPromise = (async () => {
      try {
        for await (const message of stream) {
          if (this.shuttingDown || generation !== this.resourceGeneration) {
            return;
          }
          await this.orchestrator.processMessage(message);
        }
        this.requestReconnect('Kafka message stream terminated', generation);
      } catch (error) {
        this.requestReconnect(error, generation);
      }
    })().finally(() => {
      this.processingPromises.delete(processingPromise);
    });
    this.processingPromises.add(processingPromise);
  }

  /**
   * Coalesces simultaneous current-generation failures into one reconnect task.
   *
   * @param reason - Raw lifecycle failure to sanitize for status and logging.
   * @param generation - Optional resource identity for stale-signal suppression.
   * @returns Nothing; callers share the stored reconnect promise indirectly.
   * @throws Never; reconnect-loop failures are internally contained.
   */
  private requestReconnect(reason: unknown, generation?: number): void {
    if (
      this.shuttingDown ||
      this.status.state === KafkaConnectionState.Disabled ||
      this.status.state === KafkaConnectionState.Failed ||
      (generation !== undefined && generation !== this.resourceGeneration)
    ) {
      return;
    }

    const failureReason = this.sanitizeFailureReason(reason);
    this.setStatus(
      KafkaConnectionState.Reconnecting,
      this.status.reconnectAttempts,
      failureReason,
    );
    if (this.reconnectPromise !== undefined) {
      this.reconnectRequested = true;
      this.queuedReconnectFailureReason = failureReason;
      return;
    }

    const reconnectPromise = this.runReconnectLoop(failureReason).finally(
      () => {
        if (this.reconnectPromise === reconnectPromise) {
          this.reconnectPromise = undefined;
          const queuedFailureReason = this.takeQueuedReconnectFailureReason();
          if (
            queuedFailureReason !== undefined &&
            !this.shuttingDown &&
            this.status.state !== KafkaConnectionState.Disabled &&
            this.status.state !== KafkaConnectionState.Failed
          ) {
            this.requestReconnect(queuedFailureReason);
          }
        }
      },
    );
    this.reconnectPromise = reconnectPromise;
  }

  /**
   * Recreates resources with capped exponential backoff until ready or exhausted.
   *
   * @param initialReason - Sanitized failure that initiated reconnection.
   * @returns A promise resolved on success, exhaustion, or shutdown cancellation.
   * @throws Never; each connection failure is sanitized and contained.
   */
  private async runReconnectLoop(initialReason: string): Promise<void> {
    let failureReason = initialReason;
    await this.cleanupResources();
    const kafka = this.configService.kafka;

    for (let attempt = 1; attempt <= kafka.retryAttempts; attempt += 1) {
      if (this.shuttingDown) {
        return;
      }
      this.setStatus(KafkaConnectionState.Reconnecting, attempt, failureReason);
      const completedBackoff = await this.waitForBackoff(
        this.getReconnectDelay(attempt),
      );
      if (!completedBackoff || this.shuttingDown) {
        return;
      }

      try {
        await this.establishConnection();
        const queuedFailureReason = this.takeQueuedReconnectFailureReason();
        if (queuedFailureReason !== undefined) {
          throw new Error(queuedFailureReason);
        }
        return;
      } catch (error) {
        failureReason =
          this.takeQueuedReconnectFailureReason() ??
          this.sanitizeFailureReason(error);
        await this.cleanupResources();
      }
    }

    if (!this.shuttingDown) {
      this.setStatus(
        KafkaConnectionState.Failed,
        kafka.retryAttempts,
        failureReason,
      );
      this.logger.error({
        event: 'kafka_reconnect_exhausted',
        reconnectAttempts: kafka.retryAttempts,
        failureReason,
      });
    }
  }

  /**
   * Calculates capped exponential delay for one reconnect attempt.
   *
   * @param attempt - One-based reconnect attempt number.
   * @returns Delay capped by the configured maximum retry time.
   * @throws Never; validated configuration contains positive finite integers.
   */
  private getBackoffMilliseconds(attempt: number): number {
    const kafka = this.configService.kafka;
    return Math.min(
      kafka.maxRetryTime,
      kafka.initialRetryTime * 2 ** (attempt - 1),
    );
  }

  /**
   * Calculates an equal-jitter delay within one capped exponential backoff.
   *
   * @param attempt - One-based reconnect attempt number.
   * @returns An integer between half and all of the capped delay, inclusive.
   * @throws Never; validated configuration contains positive finite integers.
   */
  private getReconnectDelay(attempt: number): number {
    const backoffMilliseconds = this.getBackoffMilliseconds(attempt);
    const minimumDelay = Math.floor(backoffMilliseconds / 2);
    const jitterRange = backoffMilliseconds - minimumDelay;
    return minimumDelay + Math.floor(Math.random() * (jitterRange + 1));
  }

  /**
   * Waits for reconnect backoff and supports prompt shutdown cancellation.
   *
   * @param milliseconds - Delay before the next connection generation.
   * @returns True after the timer, or false when shutdown cancels the wait.
   * @throws Never; cancellation resolves rather than rejects the promise.
   */
  private waitForBackoff(milliseconds: number): Promise<boolean> {
    return new Promise((resolve) => {
      let settled = false;
      const finish = (completed: boolean): void => {
        if (settled) {
          return;
        }
        settled = true;
        if (this.cancelBackoff === cancel) {
          this.cancelBackoff = undefined;
        }
        resolve(completed);
      };
      const timer = setTimeout(() => finish(true), milliseconds);
      const cancel = (): void => {
        clearTimeout(timer);
        finish(false);
      };
      this.cancelBackoff = cancel;
    });
  }

  /**
   * Detaches listeners, invalidates the generation, and closes active resources.
   *
   * @returns A promise resolved after stream and consumer closure attempts settle.
   * @throws Never; close failures are logged without propagating.
   */
  private async cleanupResources(): Promise<void> {
    this.resourceGeneration += 1;
    const stream = this.stream;
    const consumer = this.consumer;
    this.detachStreamListeners(stream);
    this.detachConsumerListeners(consumer);
    this.stream = undefined;
    this.consumer = undefined;

    if (stream !== undefined) {
      await this.closeSafely(stream, 'stream');
    }
    if (consumer !== undefined) {
      await this.closeSafely(consumer, 'consumer');
    }
  }

  /**
   * Removes listeners previously attached to the active stream.
   *
   * @param stream - Stream being retired, when one exists.
   * @returns Nothing.
   * @throws Never; listener removal is synchronous.
   */
  private detachStreamListeners(stream?: KafkaMessageStream): void {
    const listeners = this.streamListeners;
    if (stream !== undefined && listeners !== undefined) {
      stream.off('error', listeners.error);
      stream.off('autocommit', listeners.autocommit);
      stream.off('end', listeners.end);
      stream.off('close', listeners.close);
    }
    this.streamListeners = undefined;
  }

  /**
   * Removes listeners previously attached to the active consumer.
   *
   * @param consumer - Consumer being retired, when one exists.
   * @returns Nothing.
   * @throws Never; listener removal is synchronous.
   */
  private detachConsumerListeners(consumer?: KafkaConsumerClient): void {
    const listeners = this.consumerListeners;
    if (consumer !== undefined && listeners !== undefined) {
      consumer.off('error', listeners.error);
    }
    this.consumerListeners = undefined;
  }

  /**
   * Closes one Kafka resource and safely reports only its non-sensitive label.
   *
   * @param resource - Event-emitting stream or consumer with an async close.
   * @param resourceType - Safe label identifying the resource category.
   * @returns A promise resolved whether closure succeeds or fails.
   * @throws Never; resource exceptions are logged and contained.
   */
  private async closeSafely(
    resource: {
      close(force?: boolean): Promise<void>;
      on(event: string, listener: (...arguments_: any[]) => void): unknown;
      off(event: string, listener: (...arguments_: any[]) => void): unknown;
    },
    resourceType: 'stream' | 'consumer',
  ): Promise<void> {
    resource.off('error', ignoreDetachedKafkaError);
    resource.on('error', ignoreDetachedKafkaError);
    try {
      await resource.close();
      resource.off('error', ignoreDetachedKafkaError);
    } catch {
      this.logger.warn({
        event: 'kafka_resource_close_failed',
        resourceType,
      });
    }
  }

  /**
   * Removes and returns a replacement-client failure queued during reconnect.
   *
   * @returns The sanitized queued reason, or `undefined` when no new reconnect
   * was requested while the shared reconnect task was active.
   * @throws Never; the method only reads and clears internal primitive state.
   */
  private takeQueuedReconnectFailureReason(): string | undefined {
    const failureReason = this.reconnectRequested
      ? (this.queuedReconnectFailureReason ??
        'Replacement Kafka client failed during startup')
      : undefined;
    this.reconnectRequested = false;
    this.queuedReconnectFailureReason = undefined;
    return failureReason;
  }

  /**
   * Redacts configured secrets and bounds a failure reason for status exposure.
   *
   * @param reason - Arbitrary package, stream, or broker failure.
   * @returns A short reason with known PEM and passphrase values removed.
   * @throws Never; non-error inputs are converted defensively.
   */
  private sanitizeFailureReason(reason: unknown): string {
    let description =
      reason instanceof Error
        ? reason.message
        : typeof reason === 'string'
          ? reason
          : 'Unknown Kafka failure';
    const kafka = this.configService.kafka;
    for (const secret of [
      kafka.clientCert,
      kafka.clientCertKey,
      kafka.caCert,
      kafka.clientCertPassphrase,
    ]) {
      if (secret) {
        description = description.split(secret).join('[REDACTED]');
      }
    }
    description = description.replace(
      /-----BEGIN [A-Z ]+-----[\s\S]*?(?:-----END [A-Z ]+-----|$)/g,
      '[REDACTED PEM]',
    );
    const normalized = description.trim();
    return (normalized || 'Unknown Kafka failure').slice(0, 300);
  }

  /**
   * Replaces lifecycle status with a new immutable value.
   *
   * @param state - New stable Kafka connection state.
   * @param reconnectAttempts - Current externally controlled reconnect count.
   * @param failureReason - Optional already-sanitized reason for non-ready state.
   * @returns Nothing.
   * @throws Never; the method creates a frozen primitive-only object.
   */
  private setStatus(
    state: KafkaConnectionState,
    reconnectAttempts: number,
    failureReason?: string,
  ): void {
    this.status = Object.freeze({
      state,
      reconnectAttempts,
      ...(failureReason === undefined ? {} : { failureReason }),
    });
  }
}
