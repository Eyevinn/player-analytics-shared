import {
  SQSClient,
  SendMessageCommand,
  SendMessageCommandInput,
  ReceiveMessageCommandInput,
  ReceiveMessageCommand,
  DeleteMessageCommandInput,
  Message,
  DeleteMessageCommand,
} from "@aws-sdk/client-sqs";
import { AbstractQueueAdapter } from "../types/interfaces";
import winston from "winston";

export class SqsQueueAdapter implements AbstractQueueAdapter {
  logger: winston.Logger;
  client: SQSClient;

  constructor(logger: winston.Logger) {
    this.client = new SQSClient({ region: process.env.AWS_REGION });
    this.logger = logger;
  }

  async pushToQueue(event: Object): Promise<any> {
    if (process.env.SQS_QUEUE_URL === "undefined") {
      return { message: "SQS_QUEUE_URL is undefined" };
    }
    if (process.env.AWS_REGION === "undefined") {
      return { message: "AWS_REGION is undefined" };
    }
    const params: SendMessageCommandInput = {
      MessageAttributes: {
        Event: {
          DataType: "String",
          StringValue: event["event"],
        },
        Time: {
          DataType: "String",
          StringValue: event["timestamp"]
            ? event["timestamp"]
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
      this.logger.error(JSON.stringify(err));
      return err;
    }
  }

  async readFromQueue() {
    if (process.env.SQS_QUEUE_URL === "undefined") {
      return { message: "SQS_QUEUE_URL is undefined" };
    }
    if (process.env.AWS_REGION === "undefined") {
      return { message: "AWS_REGION is undefined" };
    }
    const params: ReceiveMessageCommandInput = {
      QueueUrl: process.env.SQS_QUEUE_URL,
    };
    const recieveMessageCommand = new ReceiveMessageCommand(params);
    try {
      const recieveMessageResult = await this.client.send(
        recieveMessageCommand
      );
      this.logger.info(
        `Reserved Messages From SQS Count: ${recieveMessageResult.Messages?.length}`
      );
      return recieveMessageResult;
    } catch (err) {
      this.logger.error(JSON.stringify(err));
      return err;
    }
  }
  async removeFromQueue(queueMsg: Message) {
    if (process.env.SQS_QUEUE_URL === "undefined") {
      return { message: "SQS_QUEUE_URL is undefined" };
    }
    if (process.env.AWS_REGION === "undefined") {
      return { message: "AWS_REGION is undefined" };
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
}
