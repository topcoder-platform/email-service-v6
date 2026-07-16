import { LoggerService } from '../global/logger.service';
import { EmailMessageNormalizer } from './email-message-normalizer.service';
import { EmailProcessingService } from './email-processing.service';
import { EmailPreparationSkipReason } from './email-processing.types';
import { EmailTemplateResolver } from './email-template-resolver.service';

describe('EmailProcessingService', () => {
  const normalize = jest.fn();
  const resolveTemplateId = jest.fn();
  const normalizer = {
    normalize,
  } as unknown as EmailMessageNormalizer;
  const templateResolver = {
    resolveTemplateId,
  } as unknown as EmailTemplateResolver;
  let warn: jest.SpyInstance;
  let service: EmailProcessingService;

  beforeEach(() => {
    jest.clearAllMocks();
    warn = jest.spyOn(LoggerService.prototype, 'warn').mockImplementation();
    service = new EmailProcessingService(normalizer, templateResolver);
  });

  afterEach(() => {
    warn.mockRestore();
  });

  it('returns a ready payload and resolved template without side effects', () => {
    const payload = { recipients: ['member@example.com'] };
    normalize.mockReturnValue({ ready: true, payload });
    resolveTemplateId.mockReturnValue({
      ready: true,
      templateId: 'd-template',
    });

    expect(service.prepare('notification.email', { any: 'input' })).toEqual({
      ready: true,
      payload,
      templateId: 'd-template',
    });
    expect(normalize).toHaveBeenCalledTimes(1);
    expect(resolveTemplateId).toHaveBeenCalledWith(
      'notification.email',
      payload,
    );
    expect(warn).not.toHaveBeenCalled();
  });

  it('returns a normalization skip without resolving or performing side effects', () => {
    normalize.mockReturnValue({
      ready: false,
      reason: EmailPreparationSkipReason.InvalidRecipients,
    });

    expect(
      service.prepare('notification.email', {
        recipients: [],
        secretData: 'must-not-be-logged',
      }),
    ).toEqual({
      ready: false,
      reason: EmailPreparationSkipReason.InvalidRecipients,
    });
    expect(resolveTemplateId).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalledWith({
      event: 'email_preparation_skipped',
      topic: 'notification.email',
      reason: EmailPreparationSkipReason.InvalidRecipients,
    });
    expect(JSON.stringify(warn.mock.calls)).not.toContain('must-not-be-logged');
  });

  it('returns and safely logs a template-resolution skip', () => {
    const payload = { recipients: ['member@example.com'] };
    normalize.mockReturnValue({ ready: true, payload });
    resolveTemplateId.mockReturnValue({
      ready: false,
      reason: EmailPreparationSkipReason.UnresolvedTemplate,
    });

    expect(service.prepare('unmapped.topic', payload)).toEqual({
      ready: false,
      reason: EmailPreparationSkipReason.UnresolvedTemplate,
    });
    expect(warn).toHaveBeenCalledWith({
      event: 'email_preparation_skipped',
      topic: 'unmapped.topic',
      reason: EmailPreparationSkipReason.UnresolvedTemplate,
    });
    expect(JSON.stringify(warn.mock.calls)).not.toContain('member@example.com');
  });
});
