import { getMessageQueues } from './get-message-queues';

export const obliterateAllQueues = async (): Promise<void> => {
  const { queues, close } = getMessageQueues();

  try {
    await Promise.all(queues.map((queue) => queue.obliterate({ force: true })));
  } finally {
    await close();
  }
};
