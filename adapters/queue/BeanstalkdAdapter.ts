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

  getEventJSONsFromMessages(body: any[]): Object[] {
    throw new Error("Method not implemented.");
  }
}
