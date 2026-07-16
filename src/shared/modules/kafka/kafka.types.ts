import type { Message } from '@platformatic/kafka';

/** Stable lifecycle states exposed by the Kafka consumer. */
export enum KafkaConnectionState {
  Initializing = 'initializing',
  Ready = 'ready',
  Reconnecting = 'reconnecting',
  Failed = 'failed',
  Disabled = 'disabled',
}

/** Immutable Kafka lifecycle status suitable for health reporting. */
export interface KafkaConsumerStatus {
  readonly state: KafkaConnectionState;
  readonly reconnectAttempts: number;
  readonly failureReason?: string;
}

/** Kafka message fields required by email orchestration. */
export type KafkaMessage = Pick<
  Message<Buffer, Buffer, Buffer, Buffer>,
  'key' | 'value' | 'topic' | 'partition' | 'timestamp' | 'offset'
>;

/** Consumer stream boundary used to test lifecycle behavior without a broker. */
export interface KafkaMessageStream extends AsyncIterable<KafkaMessage> {
  on(event: string, listener: (...arguments_: any[]) => void): this;
  off(event: string, listener: (...arguments_: any[]) => void): this;
  close(): Promise<void>;
}

/** Consumer boundary used by the lifecycle service and its focused tests. */
export interface KafkaConsumerClient {
  consume(options: KafkaConsumeOptions): Promise<KafkaMessageStream>;
  on(event: string, listener: (...arguments_: any[]) => void): this;
  off(event: string, listener: (...arguments_: any[]) => void): this;
  close(force?: boolean): Promise<void>;
}

/** Fetch and subscription settings supplied for every consumer generation. */
export interface KafkaConsumeOptions {
  topics: string[];
  autocommit: true;
  /** Resume committed group offsets instead of starting each stream at its end. */
  mode: 'committed';
  /** Preserve legacy first-run behavior when the group has no committed offset. */
  fallbackMode: 'latest';
  maxBytes: number;
  maxWaitTime: number;
}

/** Factory boundary that creates a fresh consumer for each lifecycle generation. */
export interface KafkaClientFactory {
  createConsumer(): Promise<KafkaConsumerClient>;
}
