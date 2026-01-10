import {
  SQSClient,
  SendMessageCommand,
  SendMessageCommandInput,
  ReceiveMessageCommandInput,
  ReceiveMessageCommand,
  DeleteMessageCommandInput,
  Message,
  DeleteMessageCommand,
  paginateListQueues,
  CreateQueueCommand,
} from '@aws-sdk/client-sqs';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { AbstractQueueAdapter } from '../../types/interfaces';
import winston from 'winston';
import { Agent } from 'http';
import { Agent as HttpsAgent } from 'https';

export interface SqsQueueAdapterOptions {
  maxSockets?: number;
  skipQueueExistsCheck?: boolean;
}

export class SqsQueueAdapter implements AbstractQueueAdapter {
  logger: winston.Logger;
  client: SQSClient;
  queueUrl: string;
  queueExists: boolean = false;
  private httpAgent: { http?: Agent; https?: HttpsAgent } = {};

  constructor(logger: winston.Logger, options?: SqsQueueAdapterOptions) {
    this.logger = logger;
    if (options?.skipQueueExistsCheck) {
      this.queueExists = true;
    }
    let region: any;
    if ('QUEUE_REGION' in process.env) {
      region = process.env.QUEUE_REGION;
    } else {
      region = process.env.AWS_REGION;
    }
    this.queueUrl = process.env.SQS_QUEUE_URL!;
    this.logger.info(`SQS Region: ${region}`);
    
    const clientConfig: any = { 
      region: region, 
      endpoint: process.env.SQS_ENDPOINT 
    };
    
    if (options?.maxSockets) {
      const httpAgent = new Agent({ 
        maxSockets: options.maxSockets, 
        keepAlive: true 
      });
      const httpsAgent = new HttpsAgent({ 
        maxSockets: options.maxSockets, 
        keepAlive: true 
      });
      
      clientConfig.requestHandler = new NodeHttpHandler({
        httpAgent,
        httpsAgent
      });
      
      this.httpAgent = { http: httpAgent, https: httpsAgent };
      this.logger.info(`SQS max sockets set to: ${options.maxSockets}`);
    }
    
    this.client = new SQSClient(clientConfig);
  }

  private async checkQueueExists(): Promise<boolean> {
    const paginatedQueues = paginateListQueues({ client: this.client }, {});
    const queues: string[] = [];

    for await (const page of paginatedQueues) {
      if (page.QueueUrls?.length) {
        queues.push(...page.QueueUrls);
      }
    }
    return queues.find((queue) => queue === this.queueUrl) ? true : false;
  }

  private async createQueue() {
    const command = new CreateQueueCommand({
      QueueName: new URL(this.queueUrl).pathname.split('/').pop()
    });
    const response = await this.client.send(command);
    this.logger.info(`Queue created: ${response.QueueUrl} (expected ${this.queueUrl})`);
  }

  private getSocketStats(): any {
    if (!this.httpAgent.http && !this.httpAgent.https) {
      return { message: 'No HTTP agent configured' };
    }
    
    const stats: any = {};
    
    if (this.httpAgent.http) {
      const agent = this.httpAgent.http as any;
      stats.http = {
        maxSockets: agent.maxSockets,
        keepAlive: agent.keepAlive,
        totalSocketCount: agent.totalSocketCount || 0,
        requests: Object.keys(agent.requests || {}).length,
        sockets: Object.keys(agent.sockets || {}).length,
        freeSockets: Object.keys(agent.freeSockets || {}).length
      };
    }
    
    if (this.httpAgent.https) {
      const agent = this.httpAgent.https as any;
      stats.https = {
        maxSockets: agent.maxSockets,
        keepAlive: agent.keepAlive,
        totalSocketCount: agent.totalSocketCount || 0,
        requests: Object.keys(agent.requests || {}).length,
        sockets: Object.keys(agent.sockets || {}).length,
        freeSockets: Object.keys(agent.freeSockets || {}).length
      };
    }
    
    return stats;
  }

  async pushToQueue(event: Object): Promise<any> {
    if (this.queueUrl === 'undefined') {
      return { message: 'SQS_QUEUE_URL is undefined' };
    }
    if (!this.queueExists) {
      this.logger.info('Checking if queue exists');
      if (!(await this.checkQueueExists())) {
        this.logger.error('Queue does not exist, creating queue');
        await this.createQueue();
        this.queueExists = true;
      } else {
        this.queueExists = true;
      }
    }
    const params: SendMessageCommandInput = {
      MessageAttributes: {
        Event: {
          DataType: 'String',
          StringValue: event['event'],
        },
        Time: {
          DataType: 'String',
          StringValue: event['timestamp']
            ? String(event['timestamp'])
            : new Date().toISOString(),
        },
      },
      QueueUrl: process.env.SQS_QUEUE_URL,
      MessageBody: JSON.stringify(event),
    };
    const sendMessageCommand = new SendMessageCommand(params);
    
    const startTime = Date.now();
    try {
      const sendMessageResult = await this.client.send(sendMessageCommand);
      const duration = Date.now() - startTime;
      
      if (duration > 2000) {
        const socketStats = this.getSocketStats();
        this.logger.warn(
          `SQS message send took ${duration}ms (>2000ms threshold). Socket stats: ${JSON.stringify(socketStats)}`
        );
      }
      
      this.logger.debug(
        `Response from SQS: ${JSON.stringify(sendMessageResult)}`
      );
      return sendMessageResult;
    } catch (err) {
      const duration = Date.now() - startTime;
      if (duration > 2000) {
        const socketStats = this.getSocketStats();
        this.logger.warn(
          `SQS message send failed after ${duration}ms (>2000ms threshold). Socket stats: ${JSON.stringify(socketStats)}`
        );
      }
      this.logger.error(err);
      return err;
    }
  }

  async pullFromQueue(): Promise<any> {
    if (this.queueUrl === 'undefined') {
      return { message: 'SQS_QUEUE_URL is undefined' };
    }
    if (!this.queueExists) {
      this.logger.info('Checking if queue exists');
      if (!(await this.checkQueueExists())) {
        this.logger.error('Queue does not exist, creating queue');
        await this.createQueue();
        this.queueExists = true;
      } else {
        this.queueExists = true;
      }
    }
    let maxMessages: number = 10;
    if (typeof process.env.SQS_MAX_MESSAGES === 'number') {
      maxMessages = process.env.SQS_MAX_MESSAGES;
    }
    let waitTime: number = 20;
    if (typeof process.env.SQS_WAIT_TIME === 'number') {
      waitTime = process.env.SQS_WAIT_TIME;
    }
    const params: ReceiveMessageCommandInput = {
      QueueUrl: process.env.SQS_QUEUE_URL,
      MaxNumberOfMessages: maxMessages,
      MessageAttributeNames: ['All'],
      WaitTimeSeconds: waitTime,
    };
    const receiveMessageCommand = new ReceiveMessageCommand(params);
    try {
      const receiveMessageResult = await this.client.send(
        receiveMessageCommand
      );
      this.logger.debug(
        `Reserved Messages From SQS Count: ${
          receiveMessageResult.Messages
            ? receiveMessageResult.Messages.length
            : 0
        }`
      );
      if (!receiveMessageResult.Messages) {
        return [];
      }
      return receiveMessageResult.Messages;
    } catch (err) {
      this.logger.error(err);
      return err;
    }
  }

  async removeFromQueue(queueMsg: Message) {
    if (process.env.SQS_QUEUE_URL === 'undefined') {
      return { message: 'SQS_QUEUE_URL is undefined' };
    }
    const params: DeleteMessageCommandInput = {
      QueueUrl: process.env.SQS_QUEUE_URL,
      ReceiptHandle: queueMsg.ReceiptHandle,
    };
    const deleteMessageCommand = new DeleteMessageCommand(params);
    try {
      const deleteMessageResult = await this.client.send(deleteMessageCommand);
      this.logger.debug(
        `Response from SQS: ${JSON.stringify(deleteMessageResult)}`
      );
      return deleteMessageResult;
    } catch (err) {
      this.logger.error(JSON.stringify(err));
      return err;
    }
  }

  getEventJSONsFromMessages(messages: Message[]): any[] {
    return messages.map((item) => (item.Body ? JSON.parse(item.Body) : {}));
  }
}
