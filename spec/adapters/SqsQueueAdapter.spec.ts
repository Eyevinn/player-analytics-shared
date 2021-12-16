import {
  DeleteMessageCommand,
  Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { AwsError, mockClient } from 'aws-sdk-client-mock';
import { SqsQueueAdapter } from '../../adapters/SqsQueueAdapter';
import Logger from '../../util/logger';

const sqsMock = mockClient(SQSClient);

describe('SQS Queue Adapter', () => {
  beforeEach(() => {
    process.env.AWS_REGION = 'us-east-1';
    process.env.QUEUE_TYPE = 'SQS';
    process.env.SQS_QUEUE_URL =
      'https://sqs.us-east-1.amazonaws.com/1234/test-queue';
    sqsMock.reset();
  });

  afterEach(() => {
    delete process.env.AWS_REGION;
    delete process.env.QUEUE_TYPE;
    delete process.env.QUEUE_REGION;
    delete process.env.SQS_QUEUE_URL;
  });

  it('should push to queue if default env is set', async () => {
    const sqsResp = { MessageId: '12345678-4444-5555-6666-111122223333' };
    const adapter = new SqsQueueAdapter(Logger);
    const event = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
    };
    sqsMock.on(SendMessageCommand).resolves(sqsResp);
    const result = await adapter.pushToQueue(event);
    expect(result).toEqual(sqsResp);
  });

  it('should push to queue if QUEUE_REGION env is set and AWS_REGION is undefined', async () => {
    process.env.QUEUE_REGION = 'eu-north-1';
    process.env.AWS_REGION = undefined;
    const sqsResp = { MessageId: '12345678-4444-5555-6666-111122223333' };
    const adapter = new SqsQueueAdapter(Logger);
    const event = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
    };
    sqsMock.on(SendMessageCommand).resolves(sqsResp);
    const result = await adapter.pushToQueue(event);
    expect(result).toEqual(sqsResp);
  });

  it('should not push to queue if sqs queue env is not set', async () => {
    process.env.SQS_QUEUE_URL = undefined;
    const queueAdapter = new SqsQueueAdapter(Logger);
    const mockEvent = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
    };
    let result = await queueAdapter.pushToQueue(mockEvent);
    expect(result).toEqual({ message: 'SQS_QUEUE_URL is undefined' });
  });

  it('should not read from queue if sqs queue env is not set', async () => {
    process.env.SQS_QUEUE_URL = undefined;
    const queueAdapter = new SqsQueueAdapter(Logger);
    let result = await queueAdapter.pullFromQueue();
    expect(result).toEqual({ message: 'SQS_QUEUE_URL is undefined' });
  });

  it('should not remove from queue if sqs queue env is not set', async () => {
    process.env.SQS_QUEUE_URL = undefined;
    const queueAdapter = new SqsQueueAdapter(Logger);
    const mockSQSMessage: Message = {
      MessageId: '62686810-05ba-4b43-62730ff3156g7jd3',
      ReceiptHandle:
        'MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3' +
        '+STFFljTM8tJJg6HRG6PYSasuWXPJB+C' +
        'wLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq' +
        '2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+' +
        'QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0=',
      MD5OfBody: 'fafb00f5732ab283681e124bf8747ed1',
      Body: JSON.stringify({
        event: 'loading',
        timestamp: 0,
        playhead: 0,
        duration: 0,
      }),
    };
    let result = await queueAdapter.removeFromQueue(mockSQSMessage);
    expect(result).toEqual({ message: 'SQS_QUEUE_URL is undefined' });
  });

  it('should catch error when sending message fails', async () => {
    const errMsg: AwsError = {
      Type: 'Sender',
      Code: 'AWS.SimpleQueueService.NonExistentQueue',
      name: 'AWS.SimpleQueueService.NonExistentQueue',
      $fault: 'client',
      $metadata: {
        httpStatusCode: 400,
        requestId: 'df840ab9-e68b-5c0e-b4a0-5094f2dfaee8',
        attempts: 1,
        totalRetryDelay: 0,
      },
    };
    const mockSQSMessage: Message = {
      MessageId: '62686810-05ba-4b43-62730ff3156g7jd3',
      ReceiptHandle:
        'MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3' +
        '+STFFljTM8tJJg6HRG6PYSasuWXPJB+C' +
        'wLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq' +
        '2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+' +
        'QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0=',
      MD5OfBody: 'fafb00f5732ab283681e124bf8747ed1',
      Body: JSON.stringify({
        event: 'loading',
        timestamp: 0,
        playhead: 0,
        duration: 0,
      }),
    };
    const mockEvent = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
    };
    sqsMock.on(SendMessageCommand).rejects(errMsg);
    sqsMock.on(ReceiveMessageCommand).rejects(errMsg);
    sqsMock.on(DeleteMessageCommand).rejects(errMsg);
    const queueAdapter = new SqsQueueAdapter(Logger);
    let pushResult = await queueAdapter.pushToQueue(mockEvent);
    let readResult = await queueAdapter.pullFromQueue();
    let removeResult = await queueAdapter.removeFromQueue(mockSQSMessage);
    expect(pushResult.toString()).toEqual(errMsg.Code);
    expect(readResult.toString()).toEqual(errMsg.Code);
    expect(removeResult.toString()).toEqual(errMsg.Code);
  });
});
