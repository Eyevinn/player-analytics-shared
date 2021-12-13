import { SQSClient } from '@aws-sdk/client-sqs';
import { SqsQueueAdapter } from '../..';
import Logger from '../../util/logger';

describe('SQS Queue Adapter', () => {
  beforeEach(() => {
    process.env.AWS_REGION = 'us-east-1';
    process.env.SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/1234/test-queue';
  });

  it('should not push to queue if sqs queue env is not set', async () => {
    process.env.SQS_QUEUE_URL = undefined;
    spyOn(SQSClient.prototype, 'send').and.callFake(function () {
      return Promise.resolve({
        message: 'SQS_QUEUE_URL is undefined',
      });
    });
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

  it('should not push to queue if AWS region env is not set', async () => {
    process.env.AWS_REGION = undefined;
    spyOn(SQSClient.prototype, 'send').and.callFake(function () {
      return Promise.resolve({
        message: 'AWS_REGION is undefined',
      });
    });
    const queueAdapter = new SqsQueueAdapter(Logger);
    const mockEvent = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
    };
    let result = await queueAdapter.pushToQueue(mockEvent);
    expect(result).toEqual({ message: 'AWS_REGION is undefined' });
  });

  it('should catch error when sending message fails', async () => {
    const errMsg = {
      Type: 'Sender',
      Code: 'AWS.SimpleQueueService.NonExistentQueue',
      Detail: '',
      name: 'AWS.SimpleQueueService.NonExistentQueue',
      $fault: 'client',
      $metadata: {
        httpStatusCode: 400,
        requestId: 'df840ab9-e68b-5c0e-b4a0-5094f2dfaee8',
        attempts: 1,
        totalRetryDelay: 0,
      },
    };
    const mockEvent = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
    };
    spyOn(SQSClient.prototype, 'send').and.callFake(function () {
      return Promise.reject(errMsg);
    });
    const queueAdapter = new SqsQueueAdapter(Logger);
    let result = await queueAdapter.pushToQueue(mockEvent);
    expect(result).toEqual(errMsg);
  });
});
