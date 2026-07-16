import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { EmailMessageNormalizer } from './email-message-normalizer.service';
import { EmailPreparationSkipReason } from './email-processing.types';

describe('EmailMessageNormalizer', () => {
  const configService = {
    email: { templateOverrideKey: 'templateOverride' },
  } as EmailServiceConfigService;
  const normalizer = new EmailMessageNormalizer(configService);
  const inputForms = [
    ['raw', (payload: Record<string, unknown>) => payload],
    [
      'bus-envelope',
      (payload: Record<string, unknown>) => ({
        topic: 'notification.email',
        payload,
      }),
    ],
  ] as const;

  it('normalizes a raw payload', () => {
    expect(
      normalizer.normalize({ recipients: ['member@example.com'] }),
    ).toEqual({
      ready: true,
      payload: { recipients: ['member@example.com'] },
    });
  });

  it('normalizes a payload from a bus envelope', () => {
    expect(
      normalizer.normalize({
        topic: 'notification.email',
        payload: { recipients: [{ email: 'member@example.com', name: 'Ada' }] },
      }),
    ).toEqual({
      ready: true,
      payload: {
        recipients: [{ email: 'member@example.com', name: 'Ada' }],
      },
    });
  });

  it('normalizes JSON text', () => {
    expect(
      normalizer.normalize(
        JSON.stringify({ recipients: ['member@example.com'], version: 'v3' }),
      ),
    ).toEqual({
      ready: true,
      payload: { recipients: ['member@example.com'], version: 'v3' },
    });
  });

  it.each([
    ['invalid JSON', '{'],
    ['null', null],
    ['array', []],
    ['number', 42],
    ['JSON null', 'null'],
    ['JSON array', '[]'],
    ['invalid envelope payload', { payload: 'not-an-object' }],
  ])('skips malformed %s input', (_label, input) => {
    expect(normalizer.normalize(input)).toEqual({
      ready: false,
      reason: EmailPreparationSkipReason.MalformedInput,
    });
  });

  it.each([
    ['missing recipients', {}],
    ['empty recipients', { recipients: [] }],
    ['primitive recipient', { recipients: [42] }],
    ['empty recipient', { recipients: [' '] }],
    ['invalid address object', { recipients: [{ name: 'Missing email' }] }],
  ])('skips payloads with %s', (_label, input) => {
    expect(normalizer.normalize(input)).toEqual({
      ready: false,
      reason: EmailPreparationSkipReason.InvalidRecipients,
    });
  });

  it('preserves every supported optional field and the configured override', () => {
    const payload = {
      recipients: ['to@example.com'],
      from: { email: 'from@example.com', name: 'Sender' },
      replyTo: 'reply@example.com',
      data: { memberName: 'Ada' },
      categories: ['account'],
      cc: ['cc@example.com'],
      bcc: [{ email: 'bcc@example.com' }],
      version: 'v3',
      attachments: [{ content: 'encoded', filename: 'notice.txt' }],
      personalizations: [
        { to: 'personalized@example.com', subject: 'Welcome' },
      ],
      sendAt: 1_700_000_000,
      templateOverride: 'd-override',
      unsupported: 'discarded',
    };

    expect(normalizer.normalize(payload)).toEqual({
      ready: true,
      payload: {
        recipients: payload.recipients,
        from: payload.from,
        replyTo: payload.replyTo,
        data: payload.data,
        categories: payload.categories,
        cc: payload.cc,
        bcc: payload.bcc,
        version: payload.version,
        attachments: payload.attachments,
        personalizations: payload.personalizations,
        sendAt: payload.sendAt,
        templateOverride: payload.templateOverride,
      },
    });
  });

  it.each([
    ['cc', 'single-cc@example.com'],
    ['bcc', { email: 'single-bcc@example.com', name: 'Blind Copy' }],
    ['categories', 'single-category'],
  ])('normalizes a singular %s value', (field, value) => {
    expect(
      normalizer.normalize({
        recipients: ['member@example.com'],
        [field]: value,
      }),
    ).toEqual({
      ready: true,
      payload: {
        recipients: ['member@example.com'],
        [field]: value,
      },
    });
  });

  it.each([
    ['from', { email: '' }],
    ['replyTo', 42],
    ['data', []],
    ['categories', ['valid', 42]],
    ['cc', 42],
    ['bcc', [null]],
    ['version', 3],
  ])('skips a structurally unusable %s field', (field, value) => {
    expect(
      normalizer.normalize({
        recipients: ['member@example.com'],
        [field]: value,
      }),
    ).toEqual({
      ready: false,
      reason: EmailPreparationSkipReason.MalformedInput,
    });
  });

  it.each([
    ['a fractional send time', { sendAt: 1_700_000_000.5 }],
    [
      'an attachment without content',
      { attachments: [{ filename: 'notice.txt' }] },
    ],
    [
      'an attachment without a filename',
      { attachments: [{ content: 'encoded' }] },
    ],
    ['empty personalizations', { personalizations: [] }],
    [
      'a personalization without recipients',
      { personalizations: [{ subject: 'Welcome' }] },
    ],
    [
      'a personalization with a fractional send time',
      {
        personalizations: [
          { to: 'personalized@example.com', sendAt: 1_700_000_000.5 },
        ],
      },
    ],
    [
      'a personalization with invalid headers',
      {
        personalizations: [
          { to: 'personalized@example.com', headers: { priority: 1 } },
        ],
      },
    ],
  ])('skips a payload with %s', (_label, optionalFields) => {
    expect(
      normalizer.normalize({
        recipients: ['member@example.com'],
        version: 'v3',
        ...optionalFields,
      }),
    ).toEqual({
      ready: false,
      reason: EmailPreparationSkipReason.MalformedInput,
    });
  });

  describe.each(inputForms)('%s payload compatibility', (_label, wrap) => {
    it.each([
      ['from', null],
      ['from', ''],
      ['from', false],
      ['from', 0],
      ['replyTo', null],
      ['replyTo', ''],
      ['replyTo', false],
      ['replyTo', 0],
      ['categories', null],
      ['categories', ''],
      ['categories', false],
      ['categories', 0],
      ['cc', null],
      ['cc', ''],
      ['cc', false],
      ['cc', 0],
      ['bcc', null],
      ['bcc', ''],
      ['bcc', false],
      ['bcc', 0],
    ])('omits a falsey %s value (%p)', (field, value) => {
      expect(
        normalizer.normalize(
          wrap({ recipients: ['member@example.com'], [field]: value }),
        ),
      ).toEqual({
        ready: true,
        payload: { recipients: ['member@example.com'] },
      });
    });

    it.each([
      ['attachments', null],
      ['attachments', ''],
      ['attachments', false],
      ['attachments', 0],
      ['personalizations', null],
      ['personalizations', ''],
      ['personalizations', false],
      ['personalizations', 0],
      ['sendAt', null],
      ['sendAt', ''],
      ['sendAt', false],
      ['sendAt', 0],
    ])('omits a falsey v3 %s value (%p)', (field, value) => {
      expect(
        normalizer.normalize(
          wrap({
            recipients: ['member@example.com'],
            version: 'v3',
            [field]: value,
          }),
        ),
      ).toEqual({
        ready: true,
        payload: {
          recipients: ['member@example.com'],
          version: 'v3',
        },
      });
    });

    it.each([
      ['attachments', ['attachment']],
      ['personalizations', [null]],
      ['sendAt', 1_700_000_000.5],
    ])('skips a truthy malformed v3 %s value', (field, value) => {
      expect(
        normalizer.normalize(
          wrap({
            recipients: ['member@example.com'],
            version: 'v3',
            [field]: value,
          }),
        ),
      ).toEqual({
        ready: false,
        reason: EmailPreparationSkipReason.MalformedInput,
      });
    });

    it.each([
      ['attachments', ['attachment']],
      ['personalizations', [null]],
      ['sendAt', 'not-a-send-time'],
    ])('ignores a malformed non-v3 %s value', (field, value) => {
      expect(
        normalizer.normalize(
          wrap({
            recipients: ['member@example.com'],
            version: 'v2',
            [field]: value,
          }),
        ),
      ).toEqual({
        ready: true,
        payload: {
          recipients: ['member@example.com'],
          version: 'v2',
        },
      });
    });
  });
});
