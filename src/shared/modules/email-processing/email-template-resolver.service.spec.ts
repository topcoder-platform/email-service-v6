import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { EmailPreparationSkipReason } from './email-processing.types';
import { EmailTemplateResolver } from './email-template-resolver.service';

describe('EmailTemplateResolver', () => {
  const configService = {
    email: {
      templateOverrideKey: 'customTemplate',
      templateMap: { 'notification.email': 'd-topic-default' },
    },
  } as EmailServiceConfigService;
  const resolver = new EmailTemplateResolver(configService);
  const recipients = ['member@example.com'];

  it('reads the exact configured override key with precedence over the topic', () => {
    expect(
      resolver.resolveTemplateId('notification.email', {
        recipients,
        customTemplate: 'd-payload-override',
        templateId: 'd-wrong-field',
      }),
    ).toEqual({ ready: true, templateId: 'd-payload-override' });
  });

  it('falls back to the current topic mapping', () => {
    expect(
      resolver.resolveTemplateId('notification.email', { recipients }),
    ).toEqual({ ready: true, templateId: 'd-topic-default' });
  });

  it.each([[''], ['   '], [42], [null], [{}]])(
    'skips a present malformed override value %#',
    (customTemplate) => {
      expect(
        resolver.resolveTemplateId('notification.email', {
          recipients,
          customTemplate,
        }),
      ).toEqual({
        ready: false,
        reason: EmailPreparationSkipReason.InvalidOverrideValue,
      });
    },
  );

  it('skips when neither an override nor topic mapping resolves', () => {
    expect(
      resolver.resolveTemplateId('unmapped.topic', { recipients }),
    ).toEqual({
      ready: false,
      reason: EmailPreparationSkipReason.UnresolvedTemplate,
    });
  });
});
