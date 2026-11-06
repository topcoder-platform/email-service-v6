import { Inject, Injectable, Optional } from '@nestjs/common';
import type {
  ConsumerOptions,
  Message,
  MessagesStream,
  ProducerOptions,
} from '@platformatic/kafka';
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import type {
  KafkaClientFactory,
  KafkaConsumerClient,
  KafkaProducerClient,
  KafkaProducerFactory,
} from './kafka.types';

/** Injection token for the native ESM package loader. */
export const PLATFORMATIC_KAFKA_MODULE_LOADER = Symbol(
  'PLATFORMATIC_KAFKA_MODULE_LOADER',
);

type PlatformaticMessage = Message<Buffer, Buffer, Buffer, Buffer>;
type PlatformaticMessagesStream = MessagesStream<
  Buffer,
  Buffer,
  Buffer,
  Buffer
> &
  AsyncIterable<PlatformaticMessage>;
type PlatformaticConsumer = KafkaConsumerClient & {
  consume(options: {
    topics: string[];
    autocommit: true;
    maxBytes: number;
    maxWaitTime: number;
  }): Promise<PlatformaticMessagesStream>;
};
type PlatformaticKafkaModule = {
  Consumer: new (
    options: ConsumerOptions<Buffer, Buffer, Buffer, Buffer>,
  ) => PlatformaticConsumer;
  Producer: new (
    options: ProducerOptions<Buffer, Buffer, Buffer, Buffer>,
  ) => KafkaProducerClient;
};

/** Loads the ESM-only Kafka package without CommonJS rewriting `import()`. */
export type PlatformaticKafkaModuleLoader =
  () => Promise<PlatformaticKafkaModule>;

// CommonJS compilation must not rewrite this native ESM loading boundary.
// eslint-disable-next-line @typescript-eslint/no-implied-eval
const nativeDynamicImport = new Function(
  'specifier',
  'return import(specifier)',
) as (specifier: string) => Promise<PlatformaticKafkaModule>;

/**
 * Loads the installed Kafka package through a native dynamic-import boundary.
 *
 * @returns The ESM module containing the Platformatic Consumer and Producer constructors.
 * @throws Propagates package resolution or ESM evaluation failures.
 */
export function loadPlatformaticKafka(): Promise<PlatformaticKafkaModule> {
  return nativeDynamicImport('@platformatic/kafka');
}

/**
 * Constructs Platformatic Kafka consumers and producers from validated
 * application settings. The consumer lifecycle service owns reconnection, so
 * package-level consumer retries are disabled.
 */
@Injectable()
export class PlatformaticKafkaClientFactory
  implements KafkaClientFactory, KafkaProducerFactory
{
  /**
   * Creates a factory with validated config and an optional testable ESM loader.
   *
   * @param configService - Validated Kafka connection and mTLS configuration.
   * @param moduleLoader - Native ESM loader, replaceable only through Nest DI.
   * @throws Never; package and TLS construction happen in createConsumer.
   */
  constructor(
    private readonly configService: EmailServiceConfigService,
    @Optional()
    @Inject(PLATFORMATIC_KAFKA_MODULE_LOADER)
    private readonly moduleLoader: PlatformaticKafkaModuleLoader = loadPlatformaticKafka,
  ) {}

  /**
   * Creates a fresh consumer with exact timeout, retry, and optional mTLS mapping.
   *
   * @returns A consumer that has not yet subscribed to any topics.
   * @throws When required identifiers or TLS material are unavailable, or when
   * the ESM package cannot load. TLS-enabled configuration never downgrades.
   */
  async createConsumer(): Promise<KafkaConsumerClient> {
    const kafka = this.configService.kafka;
    if (!kafka.clientId || !kafka.groupId) {
      throw new Error('Kafka client and group identifiers are required');
    }

    const options: ConsumerOptions<Buffer, Buffer, Buffer, Buffer> = {
      bootstrapBrokers: kafka.brokers,
      clientId: kafka.clientId,
      groupId: kafka.groupId,
      connectTimeout: kafka.connectionTimeout,
      requestTimeout: kafka.requestTimeout,
      timeout: kafka.brokerTimeout,
      sessionTimeout: kafka.sessionTimeout,
      heartbeatInterval: kafka.heartbeatInterval,
      retries: false,
      ...(kafka.sslEnabled ? { tls: this.buildTlsOptions() } : {}),
    };
    const platformaticKafka = await this.moduleLoader();
    return new platformaticKafka.Consumer(options);
  }

  /**
   * Creates a producer on the consumer's brokers, client id, timeouts, and mTLS,
   * without client-level send retries.
   *
   * @returns A producer that connects on its first send.
   * @throws When the client identifier or TLS material is unavailable, or when
   * the ESM package cannot load. TLS-enabled configuration never downgrades.
   */
  async createProducer(): Promise<KafkaProducerClient> {
    const kafka = this.configService.kafka;
    if (!kafka.clientId) {
      throw new Error('Kafka client identifier is required');
    }

    const options: ProducerOptions<Buffer, Buffer, Buffer, Buffer> = {
      bootstrapBrokers: kafka.brokers,
      clientId: kafka.clientId,
      connectTimeout: kafka.connectionTimeout,
      requestTimeout: kafka.requestTimeout,
      timeout: kafka.brokerTimeout,
      // The email-sent event is best-effort: a failed send is logged, not
      // retried, so a broker outage never holds up email delivery.
      retries: false,
      ...(kafka.sslEnabled ? { tls: this.buildTlsOptions() } : {}),
    };
    const platformaticKafka = await this.moduleLoader();
    return new platformaticKafka.Producer(options);
  }

  /**
   * Maps validated mTLS values directly to Node TLS connection option names.
   *
   * @returns TLS cert, key, and only the configured optional CA and passphrase.
   * @throws When TLS is enabled without both required client PEM values.
   */
  private buildTlsOptions(): NonNullable<
    ConsumerOptions<Buffer, Buffer, Buffer, Buffer>['tls']
  > {
    const kafka = this.configService.kafka;
    if (!kafka.clientCert || !kafka.clientCertKey) {
      throw new Error('Kafka mTLS certificate and key are required');
    }

    return {
      cert: kafka.clientCert,
      key: kafka.clientCertKey,
      ...(kafka.caCert === undefined ? {} : { ca: kafka.caCert }),
      ...(kafka.clientCertPassphrase === undefined
        ? {}
        : { passphrase: kafka.clientCertPassphrase }),
    };
  }
}
