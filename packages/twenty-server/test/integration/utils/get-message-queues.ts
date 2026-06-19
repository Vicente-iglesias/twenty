import { Queue } from 'bullmq';
import IORedis from 'ioredis';

import { MessageQueue } from 'src/engine/core-modules/message-queue/message-queue.constants';

export type MessageQueueHandles = {
  queues: Queue[];
  close: () => Promise<void>;
};

export const getMessageQueues = (): MessageQueueHandles => {
  const redisUrl = process.env.REDIS_QUEUE_URL ?? process.env.REDIS_URL;

  if (!redisUrl) {
    throw new Error('REDIS_QUEUE_URL or REDIS_URL must be defined');
  }

  const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });
  const queues = Object.values(MessageQueue).map(
    (queueName) => new Queue(queueName, { connection }),
  );

  return {
    queues,
    close: async () => {
      await connection.quit();
    },
  };
};
