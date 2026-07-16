import { EmailAttemptStatus } from '@prisma/client';
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { DEFAULT_EMAIL_RETRY_MAX_AGE_MS } from '../../../config/email-service-env';
import { EmailAttemptRepository } from './email-attempt.repository';
import { PrismaService } from './prisma.service';

describe('EmailAttemptRepository', () => {
  const retryMaxAgeMs = 60 * 60 * 1000;
  const emailAttempt = {
    create: jest.fn(),
    update: jest.fn(),
    findMany: jest.fn(),
  };
  const prisma = { emailAttempt } as unknown as PrismaService;
  const configService = {
    email: { retryMaxAgeMs },
  } as EmailServiceConfigService;
  const repository = new EmailAttemptRepository(prisma, configService);

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('creates a pending attempt with Kafka metadata', async () => {
    const messageTimestamp = new Date('2026-07-13T01:00:00.000Z');
    const input = {
      topic: 'notification.email',
      partition: 3,
      offset: '9223372036854775808',
      messageKey: 'message-1',
      messageTimestamp,
      templateId: 'template-123',
      payload: { subject: 'Welcome' },
      recipients: [{ email: 'member@example.com' }],
    };
    const created = { id: 'attempt-1', ...input };
    emailAttempt.create.mockResolvedValue(created);

    await expect(repository.createPending(input)).resolves.toBe(created);
    expect(emailAttempt.create).toHaveBeenCalledWith({
      data: {
        ...input,
        status: EmailAttemptStatus.PENDING,
      },
    });
  });

  it('marks an attempt successful using one attempt timestamp', async () => {
    const attemptedAt = new Date('2026-07-13T02:00:00.000Z');
    const updated = { id: 'attempt-1', status: EmailAttemptStatus.SUCCESS };
    emailAttempt.update.mockResolvedValue(updated);

    await expect(
      repository.markSuccess('attempt-1', attemptedAt),
    ).resolves.toBe(updated);
    expect(emailAttempt.update).toHaveBeenCalledWith({
      where: { id: 'attempt-1' },
      data: {
        status: EmailAttemptStatus.SUCCESS,
        errorMessage: null,
        sentAt: attemptedAt,
        lastAttemptedAt: attemptedAt,
      },
    });
  });

  it('marks an attempt failed without implicitly changing its retry count', async () => {
    const attemptedAt = new Date('2026-07-13T03:00:00.000Z');
    const updated = { id: 'attempt-1', status: EmailAttemptStatus.FAILED };
    emailAttempt.update.mockResolvedValue(updated);

    await expect(
      repository.markFailed('attempt-1', 'provider unavailable', attemptedAt),
    ).resolves.toBe(updated);
    expect(emailAttempt.update).toHaveBeenCalledWith({
      where: { id: 'attempt-1' },
      data: {
        status: EmailAttemptStatus.FAILED,
        errorMessage: 'provider unavailable',
        lastAttemptedAt: attemptedAt,
      },
    });
  });

  it('persists an explicit retry count for a successful retry', async () => {
    const attemptedAt = new Date('2026-07-13T02:30:00.000Z');
    emailAttempt.update.mockResolvedValue({ id: 'attempt-1' });

    await repository.markSuccess('attempt-1', attemptedAt, 3);

    expect(emailAttempt.update).toHaveBeenCalledWith({
      where: { id: 'attempt-1' },
      data: {
        status: EmailAttemptStatus.SUCCESS,
        errorMessage: null,
        sentAt: attemptedAt,
        lastAttemptedAt: attemptedAt,
        retryCount: 3,
      },
    });
  });

  it('persists an explicit retry count supplied by a retry caller', async () => {
    const attemptedAt = new Date('2026-07-13T03:00:00.000Z');
    emailAttempt.update.mockResolvedValue({ id: 'attempt-1' });

    await repository.markFailed(
      'attempt-1',
      'provider unavailable',
      attemptedAt,
      2,
    );

    expect(emailAttempt.update).toHaveBeenCalledWith({
      where: { id: 'attempt-1' },
      data: {
        status: EmailAttemptStatus.FAILED,
        errorMessage: 'provider unavailable',
        lastAttemptedAt: attemptedAt,
        retryCount: 2,
      },
    });
  });

  it('finds only failed attempts inside the configured creation-age window', async () => {
    const now = new Date('2026-07-13T04:00:00.000Z');
    const cutoff = new Date(now.getTime() - retryMaxAgeMs);
    const failedAttempts = [{ id: 'attempt-1' }];
    emailAttempt.findMany.mockResolvedValue(failedAttempts);

    await expect(repository.findRetryableFailed(now)).resolves.toBe(
      failedAttempts,
    );
    expect(emailAttempt.findMany).toHaveBeenCalledWith({
      where: {
        status: EmailAttemptStatus.FAILED,
        createdAt: { gte: cutoff },
      },
      orderBy: [{ createdAt: 'asc' }, { id: 'asc' }],
    });
    expect(emailAttempt.findMany.mock.calls[0][0].where.status).not.toBe(
      EmailAttemptStatus.PENDING,
    );
  });

  it('uses the default 24-hour cutoff for retry eligibility', async () => {
    const now = new Date('2026-07-13T04:00:00.000Z');
    const defaultConfigService = {
      email: { retryMaxAgeMs: DEFAULT_EMAIL_RETRY_MAX_AGE_MS },
    } as EmailServiceConfigService;
    const defaultRepository = new EmailAttemptRepository(
      prisma,
      defaultConfigService,
    );
    emailAttempt.findMany.mockResolvedValue([]);

    await defaultRepository.findRetryableFailed(now);

    expect(emailAttempt.findMany).toHaveBeenCalledWith({
      where: {
        status: EmailAttemptStatus.FAILED,
        createdAt: {
          gte: new Date(now.getTime() - DEFAULT_EMAIL_RETRY_MAX_AGE_MS),
        },
      },
      orderBy: [{ createdAt: 'asc' }, { id: 'asc' }],
    });
  });
});
