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
import { AbstractQueueAdapter } from '../../types/interfaces';
import winston from 'winston';

export class SqsQueueAdapter implements AbstractQueueAdapter {
  logger: winston.Logger;
  client: SQSClient;
  queueUrl: string;
  queueExists: boolean = false;

  constructor(logger: winston.Logger) {
    this.logger = logger;
    let region: any;
    if ('QUEUE_REGION' in process.env) {
      region = process.env.QUEUE_REGION;
    } else {
      region = process.env.AWS_REGION;
    }
    if (process.env.SQS_QUEUE_URL === 'undefined') {
      throw new Error('SQS_QUEUE_URL is undefined');
    }
    this.queueUrl = process.env.SQS_QUEUE_URL!;
    this.logger.info(`SQS Region: ${region}`);
    this.client = new SQSClient({ region: region, endpoint: process.env.SQS_ENDPOINT });
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

  async pushToQueue(event: Object): Promise<any> {
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
            ? event['timestamp']
            : new Date().toISOString(),
        },
      },
      QueueUrl: process.env.SQS_QUEUE_URL,
      MessageBody: JSON.stringify(event),
    };
    const sendMessageCommand = new SendMessageCommand(params);
    try {
      const sendMessageResult = await this.client.send(sendMessageCommand);
      this.logger.info(
        `Response from SQS: ${JSON.stringify(sendMessageResult)}`
      );
      return sendMessageResult;
    } catch (err) {
      this.logger.error(err);
      return err;
    }
  }

  async pullFromQueue(): Promise<any> {
    if (process.env.SQS_QUEUE_URL === 'undefined') {
      return { message: 'SQS_QUEUE_URL is undefined' };
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
      this.logger.info(
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
      this.logger.info(
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
