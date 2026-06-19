import { type JobType, type Queue } from 'bullmq';
import { isDefined } from 'twenty-shared/utils';

import { getMessageQueues } from './get-message-queues';

const PENDING_JOB_STATUSES = [
  'wait',
  'active',
  'delayed',
  'prioritized',
  'waiting-children',
] as const satisfies JobType[];

type WaitForQueueIdleOptions = {
  timeoutMs?: number;
  idleMs?: number;
  pollMs?: number;
  throwOnFailure?: boolean;
};

const areFakeTimersActive = (): boolean => {
  if (typeof jest === 'undefined') {
    return false;
  }

  try {
    jest.getRealSystemTime();

    return true;
  } catch {
    return false;
  }
};

const getPendingCountByQueue = async (
  queues: Queue[],
): Promise<Record<string, number>> => {
  const entries = await Promise.all(
    queues.map(async (queue) => {
      const counts = await queue.getJobCounts(...PENDING_JOB_STATUSES);
      const pendingCount = PENDING_JOB_STATUSES.reduce(
        (sum, status) => sum + (counts[status] ?? 0),
        0,
      );

      return [queue.name, pendingCount] as const;
    }),
  );

  return Object.fromEntries(entries);
};

const collectFailedJobReasons = async (
  queues: Queue[],
  failedAfter: number,
): Promise<string[]> => {
  const failedJobsByQueue = await Promise.all(
    queues.map((queue) => queue.getFailed()),
  );

  return failedJobsByQueue
    .flat()
    .filter((job) => isDefined(job.finishedOn) && job.finishedOn >= failedAfter)
    .map((job) => `${job.queueName}/${job.name}: ${job.failedReason}`);
};

export const waitForQueueIdle = async ({
  timeoutMs = 30_000,
  idleMs = 250,
  pollMs = 25,
  throwOnFailure = true,
}: WaitForQueueIdleOptions = {}): Promise<void> => {
  const shouldRestoreFakeTimers = areFakeTimersActive();

  if (shouldRestoreFakeTimers) {
    jest.useRealTimers();
  }

  const { queues, close } = getMessageQueues();
  const startedAt = Date.now();
  const deadline = startedAt + timeoutMs;
  let idleSince: number | null = null;

  try {
    for (;;) {
      const pendingCountByQueue = await getPendingCountByQueue(queues);
      const totalPendingCount = Object.values(pendingCountByQueue).reduce(
        (total, count) => total + count,
        0,
      );

      if (totalPendingCount === 0) {
        idleSince ??= Date.now();

        if (Date.now() - idleSince >= idleMs) {
          break;
        }
      } else {
        idleSince = null;
      }

      if (Date.now() >= deadline) {
        const stuckQueues = Object.entries(pendingCountByQueue)
          .filter(([, count]) => count > 0)
          .map(([queueName, count]) => `${queueName}=${count}`)
          .join(', ');

        throw new Error(
          `Queues did not become idle within ${timeoutMs}ms. Pending: ${stuckQueues}`,
        );
      }

      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }

    if (throwOnFailure) {
      const failedJobReasons = await collectFailedJobReasons(queues, startedAt);

      if (failedJobReasons.length > 0) {
        throw new Error(`BullMQ jobs failed: ${failedJobReasons.join('; ')}`);
      }
    }
  } finally {
    await close();

    if (shouldRestoreFakeTimers) {
      jest.useFakeTimers();
    }
  }
};
