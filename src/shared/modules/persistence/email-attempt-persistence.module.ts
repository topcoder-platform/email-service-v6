import { Module } from '@nestjs/common';
import { EmailAttemptRepository } from './email-attempt.repository';
import { PrismaService } from './prisma.service';

/**
 * Registers and exports the Prisma lifecycle provider and email-attempt
 * repository for Kafka delivery, retry scheduling, and live health probing.
 */
@Module({
  providers: [PrismaService, EmailAttemptRepository],
  exports: [PrismaService, EmailAttemptRepository],
})
export class EmailAttemptPersistenceModule {}
