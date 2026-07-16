// Match the SendGrid package's direct CommonJS export used by production.
// eslint-disable-next-line @typescript-eslint/no-require-imports
import sendGridMail = require('@sendgrid/mail');
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { LoggerService } from '../global/logger.service';
import { SendGridDeliveryService } from './sendgrid-delivery.service';

jest.mock('@sendgrid/mail', () => ({
  setApiKey: jest.fn(),
  send: jest.fn(),
}));

describe('SendGridDeliveryService', () => {
  const apiKey = 'SG.secret-value.signature';
  const configService = {
    email: {
      sendGridApiKey: apiKey,
      from: 'default@example.com',
    },
  } as EmailServiceConfigService;
  // SDK methods are intentionally captured as Jest mocks for isolated mapping tests.
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const setApiKey = jest.mocked(sendGridMail.setApiKey);
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const send = jest.mocked(sendGridMail.send);
  let log: jest.SpyInstance;
  let service: SendGridDeliveryService;

  beforeEach(() => {
    jest.clearAllMocks();
    log = jest.spyOn(LoggerService.prototype, 'log').mockImplementation();
    service = new SendGridDeliveryService(configService);
  });

  afterEach(() => {
    log.mockRestore();
  });

  it('initializes the client with the API key without logging it', () => {
    expect(setApiKey).toHaveBeenCalledWith(apiKey);
    expect(JSON.stringify(log.mock.calls)).not.toContain(apiKey);
  });

  it('defaults omitted sender, copy, and category fields', async () => {
    const result = [{ statusCode: 202 }, {}];
    send.mockResolvedValue(result as never);

    await service.send('d-template', {
      recipients: ['member@example.com'],
    });

    expect(send).toHaveBeenCalledWith(
      expect.objectContaining({
        from: 'default@example.com',
        replyTo: 'default@example.com',
        categories: [],
        cc: [],
        bcc: [],
      }),
    );
  });

  it('maps every supported v3 optional field together', async () => {
    send.mockResolvedValue([{ statusCode: 202 }, {}] as never);
    const payload = {
      recipients: [{ email: 'to@example.com', name: 'Recipient' }],
      from: { email: 'from@example.com', name: 'Sender' },
      replyTo: 'reply@example.com',
      data: { memberName: 'Ada' },
      categories: ['account', 'welcome'],
      cc: ['cc@example.com'],
      bcc: [{ email: 'bcc@example.com' }],
      version: 'v3',
      attachments: [{ content: 'encoded', filename: 'notice.txt' }],
      personalizations: [
        { to: 'personalized@example.com', subject: 'Welcome Ada' },
      ],
      sendAt: 1_700_000_000,
    };

    await service.send('d-v3-template', payload);

    expect(send).toHaveBeenCalledWith({
      to: payload.recipients,
      templateId: 'd-v3-template',
      dynamicTemplateData: payload.data,
      personalizations: payload.personalizations,
      from: payload.from,
      replyTo: payload.replyTo,
      categories: payload.categories,
      cc: payload.cc,
      bcc: payload.bcc,
      attachments: payload.attachments,
      sendAt: payload.sendAt,
    });
    expect(JSON.stringify(log.mock.calls)).not.toContain('Ada');
    expect(JSON.stringify(log.mock.calls)).not.toContain('to@example.com');
  });

  it('maps legacy substitutions and wrappers without v3-only fields', async () => {
    send.mockResolvedValue([{ statusCode: 202 }, {}] as never);
    const payload = {
      recipients: ['member@example.com'],
      from: 'sender@example.com',
      replyTo: { email: 'reply@example.com', name: 'Support' },
      data: { firstName: 'Grace' },
      categories: ['legacy'],
      cc: ['cc@example.com'],
      bcc: ['bcc@example.com'],
      version: 'v2',
      attachments: [{ content: 'ignored', filename: 'ignored.txt' }],
      personalizations: [
        { to: 'personalized@example.com', subject: 'ignored' },
      ],
      sendAt: 1_700_000_000,
    };

    await service.send('legacy-template', payload);

    const request = send.mock.calls[0][0];
    expect(request).toEqual({
      to: payload.recipients,
      templateId: 'legacy-template',
      substitutions: payload.data,
      substitutionWrappers: ['{{', '}}'],
      from: payload.from,
      replyTo: payload.replyTo,
      categories: payload.categories,
      cc: payload.cc,
      bcc: payload.bcc,
    });
    expect(request).not.toHaveProperty('dynamicTemplateData');
    expect(request).not.toHaveProperty('attachments');
    expect(request).not.toHaveProperty('personalizations');
    expect(request).not.toHaveProperty('sendAt');
  });

  it.each([
    ['cc', 'single-cc@example.com'],
    ['bcc', { email: 'single-bcc@example.com', name: 'Blind Copy' }],
    ['categories', 'single-category'],
  ])(
    'preserves a singular %s value in the SendGrid request',
    async (field, value) => {
      send.mockResolvedValue([{ statusCode: 202 }, {}] as never);

      await service.send('d-template', {
        recipients: ['member@example.com'],
        [field]: value,
      });

      expect(send).toHaveBeenCalledWith(
        expect.objectContaining({ [field]: value }),
      );
    },
  );

  it('propagates SendGrid delivery failures without logging the error', async () => {
    const error = new Error('provider unavailable');
    send.mockRejectedValue(error);

    await expect(
      service.send('d-template', { recipients: ['member@example.com'] }),
    ).rejects.toBe(error);
    expect(JSON.stringify(log.mock.calls)).not.toContain(error.message);
  });
});
