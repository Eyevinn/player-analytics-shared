import RedisTaskQueue = require("redis-task-queue");
import winston from "winston";
import { AbstractQueueAdapter } from "../../types/interfaces";

export class RedisAdapter implements AbstractQueueAdapter {
  logger: winston.Logger;
  client: RedisTaskQueue;

  constructor(logger: winston.Logger) {
    this.logger = logger;
    this.client = new RedisTaskQueue();
  }

  async pushToQueue(body: Object): Promise<Object> {
    const result = await this.client.add({ data: body });
    return result;
  }

  async pullFromQueue(): Promise<Object> {
    const job = await this.client.get();
    return job || {};
  }

  async removeFromQueue(body: Record<string, any>): Promise<boolean> {
    const jobId = body.id;
    const result = await this.client.getStatus(jobId);
    return result === 'completed';
  }

  getEventJSONsFromMessages(body: any[]): Object[] {
    this.logger.warn("Method not implemented.");
  }
}
