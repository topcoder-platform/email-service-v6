import { Global, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { EmailServiceConfigService } from './email-service-config.service';
import { validateEmailServiceEnv } from './email-service-env';

/**
 * Registers fail-fast environment validation and typed configuration access for
 * the email service. External clients remain outside this foundation module.
 */
@Global()
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      cache: true,
      validate: validateEmailServiceEnv,
    }),
  ],
  providers: [EmailServiceConfigService],
  exports: [EmailServiceConfigService],
})
export class EmailServiceConfigModule {}
