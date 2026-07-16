-- CreateEnum
CREATE TYPE "EmailAttemptStatus" AS ENUM ('PENDING', 'SUCCESS', 'FAILED');

-- CreateTable
CREATE TABLE "EmailAttempt" (
    "id" UUID NOT NULL,
    "topic" TEXT NOT NULL,
    "partition" INTEGER,
    "offset" TEXT,
    "messageKey" TEXT,
    "messageTimestamp" TIMESTAMP(3),
    "templateId" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "recipients" JSONB NOT NULL,
    "status" "EmailAttemptStatus" NOT NULL DEFAULT 'PENDING',
    "errorMessage" TEXT,
    "retryCount" INTEGER NOT NULL DEFAULT 0,
    "lastAttemptedAt" TIMESTAMP(3),
    "sentAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "EmailAttempt_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "EmailAttempt_status_createdAt_idx" ON "EmailAttempt"("status", "createdAt");

-- CreateIndex
CREATE INDEX "EmailAttempt_topic_createdAt_idx" ON "EmailAttempt"("topic", "createdAt");

-- CreateIndex
CREATE INDEX "EmailAttempt_topic_partition_offset_idx" ON "EmailAttempt"("topic", "partition", "offset");
