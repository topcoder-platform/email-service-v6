// Exercise the SendGrid package's actual direct CommonJS export shape.
// eslint-disable-next-line @typescript-eslint/no-require-imports
import sendGridMail = require('@sendgrid/mail');
import { EmailServiceConfigService } from '../../../config/email-service-config.service';
import { SendGridDeliveryService } from './sendgrid-delivery.service';

describe('SendGridDeliveryService runtime integration', () => {
  it('constructs the provider against the actual CommonJS SendGrid export', () => {
    const configService = {
      email: {
        sendGridApiKey: 'SG.runtime-shape.signature',
        from: 'default@example.com',
      },
    } as EmailServiceConfigService;

    expect(typeof sendGridMail.setApiKey).toBe('function');
    expect(() => new SendGridDeliveryService(configService)).not.toThrow();
  });
});
