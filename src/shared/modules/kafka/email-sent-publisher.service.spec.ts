import type { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { LoggerService } from '../global/logger.service';
import { EmailSentPublisherService } from './email-sent-publisher.service';
import type { KafkaProducerFactory } from './kafka.types';

/**
 * Creates config access with the Kafka switch and email-sent topic.
 *
 * @param overrides - Kafka values that replace the enabled defaults.
 * @returns A config-service double accepted by the publisher.
 */
function createConfig(
  overrides: Record<string, unknown> = {},
): EmailServiceConfigService {
  return {
    kafka: {
      disabled: false,
      emailSentTopic: 'external.action.email.sent',
      ...overrides,
    },
  } as unknown as EmailServiceConfigService;
}

describe('EmailSentPublisherService', () => {
  const send = jest.fn();
  const close = jest.fn();
  const createProducer = jest.fn();
  const factory: KafkaProducerFactory = { createProducer };
  const event = {
    correlationId: '4f0c2a1e-6b7d-4e8f-9a01-2b3c4d5e6f70:lets-talk-sales-reply',
    sendgridMessageId: 'sg-message-1',
    templateId: 'd-template',
    sentAt: new Date('2026-11-04T10:00:00.000Z'),
  };
  let errorLog: jest.SpyInstance;
  let infoLog: jest.SpyInstance;

  beforeEach(() => {
    jest.clearAllMocks();
    send.mockResolvedValue({});
    close.mockResolvedValue(undefined);
    createProducer.mockResolvedValue({ send, close });
    errorLog = jest
      .spyOn(LoggerService.prototype, 'error')
      .mockImplementation();
    infoLog = jest.spyOn(LoggerService.prototype, 'log').mockImplementation();
  });

  afterEach(() => {
    errorLog.mockRestore();
    infoLog.mockRestore();
  });

  it('produces one Bus envelope keyed by the correlation id', async () => {
    const service = new EmailSentPublisherService(createConfig(), factory);

    await service.publish(event);

    expect(send).toHaveBeenCalledTimes(1);
    const [message] = send.mock.calls[0][0].messages;
    expect(message.topic).toBe('external.action.email.sent');
    expect(message.key.toString('utf8')).toBe(event.correlationId);
    expect(JSON.parse(message.value.toString('utf8'))).toEqual({
      topic: 'external.action.email.sent',
      originator: 'tc-email-service',
      timestamp: expect.any(String),
      'mime-type': 'application/json',
      payload: {
        correlationId: event.correlationId,
        sendgridMessageId: 'sg-message-1',
        templateId: 'd-template',
        sentAt: '2026-11-04T10:00:00.000Z',
      },
    });
  });

  it('creates the producer once and closes it on shutdown', async () => {
    const service = new EmailSentPublisherService(createConfig(), factory);

    await service.publish(event);
    await service.publish({ ...event, sendgridMessageId: null });
    await service.onModuleDestroy();

    expect(createProducer).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledTimes(2);
    expect(close).toHaveBeenCalledTimes(1);
  });

  it.each([
    ['Kafka is disabled', { disabled: true }],
    ['the topic is empty', { emailSentTopic: '' }],
  ])('publishes nothing when %s', async (_case, overrides) => {
    const service = new EmailSentPublisherService(
      createConfig(overrides),
      factory,
    );

    await service.publish(event);
    await service.onModuleDestroy();

    expect(createProducer).not.toHaveBeenCalled();
    expect(close).not.toHaveBeenCalled();
  });

  it('contains producer creation and send failures and retries creation later', async () => {
    const service = new EmailSentPublisherService(createConfig(), factory);
    createProducer.mockRejectedValueOnce(new Error('broker unreachable'));

    await expect(service.publish(event)).resolves.toBeUndefined();
    send.mockRejectedValueOnce(new Error('produce failed'));
    await expect(service.publish(event)).resolves.toBeUndefined();

    expect(createProducer).toHaveBeenCalledTimes(2);
    expect(errorLog).toHaveBeenCalledTimes(2);
    expect(errorLog).toHaveBeenCalledWith({
      event: 'email_sent_event_publish_failed',
      topic: 'external.action.email.sent',
    });
    expect(JSON.stringify(errorLog.mock.calls)).not.toContain(
      event.correlationId,
    );
  });
});
