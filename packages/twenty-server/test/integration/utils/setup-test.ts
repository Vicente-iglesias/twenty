import { type JestConfigWithTsJest } from 'ts-jest';
import 'tsconfig-paths/register';

import { rawDataSource } from 'src/database/typeorm/raw/raw.datasource';
import { MessageQueueDriverType } from 'src/engine/core-modules/message-queue/interfaces/message-queue-module-options.interface';

import { createApp } from './create-app';
import { obliterateAllQueues } from './obliterate-all-queues';

export default async (_: unknown, projectConfig: JestConfigWithTsJest) => {
  const app = await createApp({});

  if (!projectConfig.globals) {
    throw new Error('No globals found in project config');
  }

  await rawDataSource.initialize();

  await app.listen(projectConfig.globals.APP_PORT as number);

  global.app = app;
  global.testDataSource = rawDataSource;

  if (process.env.MESSAGE_QUEUE_TYPE === MessageQueueDriverType.BullMQ) {
    await obliterateAllQueues();
  }
};
