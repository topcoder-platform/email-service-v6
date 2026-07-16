import { LoggerService } from '../global/logger.service';
import { KafkaEmailOrchestratorService } from './kafka-email-orchestrator.service';
import type { KafkaMessage } from './kafka.types';

/**
 * Creates a complete Kafka message for focused orchestration tests.
 *
 * @param overrides - Metadata or content replacements for a test case.
 * @returns A typed Kafka message with deterministic correlation fields.
 */
function createMessage(overrides: Partial<KafkaMessage> = {}): KafkaMessage {
  return {
    topic: 'notifications.email',
    partition: 3,
    offset: 9_223_372_036_854_775_808n,
    timestamp: 1_783_909_800_000n,
    key: Buffer.from('message-1'),
    value: Buffer.from('{"recipients":["member@example.com"]}'),
    ...overrides,
  };
}

describe('KafkaEmailOrchestratorService', () => {
  const payload = {
    recipients: ['member@example.com'],
    data: { privateValue: 'payload-secret' },
  };
  const prepare = jest.fn();
  const send = jest.fn();
  const createPending = jest.fn();
  const markSuccess = jest.fn();
  const markFailed = jest.fn();
  const processingService = { prepare };
  const deliveryService = { send };
  const attemptRepository = { createPending, markSuccess, markFailed };
  let errorLog: jest.SpyInstance;
  let service: KafkaEmailOrchestratorService;

  beforeEach(() => {
    jest.clearAllMocks();
    prepare.mockReturnValue({ ready: true, templateId: 'd-template', payload });
    createPending.mockResolvedValue({ id: 'attempt-1' });
    send.mockResolvedValue([{ statusCode: 202 }]);
    markSuccess.mockResolvedValue({ id: 'attempt-1' });
    markFailed.mockResolvedValue({ id: 'attempt-1' });
    errorLog = jest
      .spyOn(LoggerService.prototype, 'error')
      .mockImplementation();
    service = new KafkaEmailOrchestratorService(
      processingService as never,
      deliveryService as never,
      attemptRepository as never,
    );
  });

  afterEach(() => {
    errorLog.mockRestore();
  });

  it('returns on a preparation skip without persistence or delivery', async () => {
    prepare.mockReturnValue({ ready: false, reason: 'malformed_input' });

    await expect(
      service.processMessage(createMessage()),
    ).resolves.toBeUndefined();

    expect(createPending).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  });

  it('persists Kafka metadata and PENDING content before SendGrid delivery', async () => {
    await service.processMessage(createMessage());

    expect(prepare).toHaveBeenCalledWith(
      'notifications.email',
      '{"recipients":["member@example.com"]}',
    );
    expect(createPending).toHaveBeenCalledWith({
      topic: 'notifications.email',
      partition: 3,
      offset: '9223372036854775808',
      messageKey: 'message-1',
      messageTimestamp: new Date('2026-07-13T02:30:00.000Z'),
      templateId: 'd-template',
      payload,
      recipients: payload.recipients,
    });
    expect(createPending.mock.invocationCallOrder[0]).toBeLessThan(
      send.mock.invocationCallOrder[0],
    );
    expect(send).toHaveBeenCalledWith('d-template', payload);
  });

  it('omits an invalid Kafka timestamp safely', async () => {
    await service.processMessage(
      createMessage({ timestamp: 8_640_000_000_000_001n }),
    );

    expect(createPending.mock.calls[0][0]).not.toHaveProperty(
      'messageTimestamp',
    );
  });

  it('continues delivery after PENDING persistence fails and logs safely', async () => {
    createPending.mockRejectedValue(
      new Error(
        'database included member@example.com payload-secret PRIVATE-KEY passphrase',
      ),
    );

    await expect(
      service.processMessage(createMessage()),
    ).resolves.toBeUndefined();

    expect(send).toHaveBeenCalledWith('d-template', payload);
    expect(markSuccess).not.toHaveBeenCalled();
    expect(errorLog).toHaveBeenCalledWith({
      event: 'email_pending_persistence_failed',
      severity: 'critical',
      topic: 'notifications.email',
      partition: 3,
      offset: '9223372036854775808',
      hasMessageKey: true,
    });
    const logged = JSON.stringify(errorLog.mock.calls);
    expect(logged).not.toContain('member@example.com');
    expect(logged).not.toContain('payload-secret');
    expect(logged).not.toContain('message-1');
    expect(logged).not.toContain('PRIVATE-KEY');
    expect(logged).not.toContain('passphrase');
  });

  it('marks a persisted attempt SUCCESS after accepted delivery', async () => {
    await service.processMessage(createMessage());

    expect(markSuccess).toHaveBeenCalledWith('attempt-1');
    expect(markFailed).not.toHaveBeenCalled();
  });

  it('marks a persisted attempt FAILED with a stable safe description', async () => {
    send.mockRejectedValue(
      new Error('rejected member@example.com payload-secret message-1'),
    );

    await expect(
      service.processMessage(createMessage()),
    ).resolves.toBeUndefined();

    expect(markFailed).toHaveBeenCalledWith(
      'attempt-1',
      'SendGrid delivery failed',
    );
    const logged = JSON.stringify(errorLog.mock.calls);
    expect(logged).not.toContain('member@example.com');
    expect(logged).not.toContain('payload-secret');
    expect(logged).not.toContain('message-1');
  });

  it('contains SUCCESS and FAILED status-update failures', async () => {
    markSuccess.mockRejectedValueOnce(new Error('success update failed'));
    await expect(
      service.processMessage(createMessage()),
    ).resolves.toBeUndefined();

    send.mockRejectedValueOnce(new Error('provider failed'));
    markFailed.mockRejectedValueOnce(new Error('failure update failed'));
    await expect(
      service.processMessage(createMessage()),
    ).resolves.toBeUndefined();

    expect(errorLog).toHaveBeenCalledWith(
      expect.objectContaining({ event: 'email_success_persistence_failed' }),
    );
    expect(errorLog).toHaveBeenCalledWith(
      expect.objectContaining({ event: 'email_failure_persistence_failed' }),
    );
  });
});
