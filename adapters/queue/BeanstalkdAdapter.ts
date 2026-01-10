import { Client } from "node-beanstalk";
import winston from "winston";
import { AbstractQueueAdapter } from "../../types/interfaces";

export class BeanstalkdAdapter implements AbstractQueueAdapter {
  logger: winston.Logger;
  client: Client;

  constructor(logger: winston.Logger) {
    this.logger = logger;
    this.client = new Client();
  }

  private async connect(): Promise<void> {
    await this.client.connect();
  }

  async pushToQueue(body: Object): Promise<Object> {
    if (!this.client.isConnected) await this.connect();
    const result = await this.client.put(body);
    return result;
  }

  async pullFromQueue(): Promise<Object> {
    if (!this.client.isConnected) await this.connect();
    const job = await this.client.reserveWithTimeout(1);
    return job || {};
  }

  async removeFromQueue(body: Record<string, any>): Promise<boolean> {
    if (!this.client.isConnected) await this.connect();
    const jobId = body.id;
    const result = await this.client.delete(jobId);
    return result;
  }

  async removeFromQueueBatch(messages: Record<string, any>[]): Promise<Object> {
    const results: { successful: any[]; failed: any[] } = {
      successful: [],
      failed: [],
    };

    for (const message of messages) {
      try {
        const success = await this.removeFromQueue(message);
        if (success) {
          results.successful.push({ id: message.id });
        } else {
          results.failed.push({ id: message.id, reason: 'delete failed' });
        }
      } catch (err) {
        results.failed.push({
          id: message.id,
          reason: err instanceof Error ? err.message : 'Unknown error',
        });
      }
    }

    return results;
  }

  getEventJSONsFromMessages(body: any[]): Object[] {
    this.logger.warn("Method not implemented.");
    return body;
  }
}
