import {
  DeleteMessageCommand,
  DeleteMessageBatchCommand,
  Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { AwsError, mockClient } from 'aws-sdk-client-mock';
import { SqsQueueAdapter } from '../../adapters/queue/SqsQueueAdapter';
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
    // Using any type assertion to bypass TypeScript's type checking on spyOn
    spyOn(adapter as any, 'checkQueueExists').and.returnValue(true);
    sqsMock.on(SendMessageCommand).resolves(sqsResp);
    const result = await adapter.pushToQueue(event);
    expect(result).toEqual(sqsResp);
  });

  it('should create adapter with custom max sockets configuration', async () => {
    const adapter = new SqsQueueAdapter(Logger, { maxSockets: 100 });
    expect(adapter).toBeDefined();
    expect(adapter.client).toBeDefined();
  });

  it('should skip queue exists check when skipQueueExistsCheck option is set', async () => {
    const sqsResp = { MessageId: '12345678-4444-5555-6666-111122223333' };
    const adapter = new SqsQueueAdapter(Logger, { skipQueueExistsCheck: true });
    expect(adapter.queueExists).toBe(true);
    const event = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
    };
    const checkQueueExistsSpy = spyOn(adapter as any, 'checkQueueExists');
    sqsMock.on(SendMessageCommand).resolves(sqsResp);
    const result = await adapter.pushToQueue(event);
    expect(result).toEqual(sqsResp);
    expect(checkQueueExistsSpy).not.toHaveBeenCalled();
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
    // Using any type assertion to bypass TypeScript's type checking on spyOn
    spyOn(adapter as any, 'checkQueueExists').and.returnValue(true);
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
    
    // Using any type assertion to bypass TypeScript's type checking on spyOn
    spyOn(queueAdapter as any, 'checkQueueExists').and.returnValue(true);

    let pushResult = await queueAdapter.pushToQueue(mockEvent);
    let readResult = await queueAdapter.pullFromQueue();
    let removeResult = await queueAdapter.removeFromQueue(mockSQSMessage);
    expect(pushResult.toString()).toEqual(errMsg.Code);
    expect(readResult.toString()).toEqual(errMsg.Code);
    expect(removeResult.toString()).toEqual(errMsg.Code);
  });

  it('should batch remove messages from queue', async () => {
    const batchResp = {
      Successful: [{ Id: '0' }, { Id: '1' }],
      Failed: [],
    };
    sqsMock.on(DeleteMessageBatchCommand).resolves(batchResp);
    const queueAdapter = new SqsQueueAdapter(Logger, { skipQueueExistsCheck: true });
    const messages: Message[] = [
      {
        MessageId: 'msg-1',
        ReceiptHandle: 'receipt-1',
        Body: JSON.stringify({ event: 'loading' }),
      },
      {
        MessageId: 'msg-2',
        ReceiptHandle: 'receipt-2',
        Body: JSON.stringify({ event: 'playing' }),
      },
    ];
    const result = await queueAdapter.removeFromQueueBatch(messages);
    expect(result).toEqual({ successful: batchResp.Successful, failed: [] });
  });

  it('should handle empty messages array in batch remove', async () => {
    const queueAdapter = new SqsQueueAdapter(Logger, { skipQueueExistsCheck: true });
    const result = await queueAdapter.removeFromQueueBatch([]);
    expect(result).toEqual({ successful: [], failed: [] });
  });

  it('should not batch remove from queue if sqs queue env is not set', async () => {
    process.env.SQS_QUEUE_URL = undefined;
    const queueAdapter = new SqsQueueAdapter(Logger);
    const messages: Message[] = [
      {
        MessageId: 'msg-1',
        ReceiptHandle: 'receipt-1',
        Body: JSON.stringify({ event: 'loading' }),
      },
    ];
    const result = await queueAdapter.removeFromQueueBatch(messages);
    expect(result).toEqual({ message: 'SQS_QUEUE_URL is undefined' });
  });

  it('should handle batch delete with more than 10 messages', async () => {
    const batchResp = {
      Successful: Array.from({ length: 10 }, (_, i) => ({ Id: `${i}` })),
      Failed: [],
    };
    sqsMock.on(DeleteMessageBatchCommand).resolves(batchResp);
    const queueAdapter = new SqsQueueAdapter(Logger, { skipQueueExistsCheck: true });
    const messages: Message[] = Array.from({ length: 15 }, (_, i) => ({
      MessageId: `msg-${i}`,
      ReceiptHandle: `receipt-${i}`,
      Body: JSON.stringify({ event: 'loading' }),
    }));
    const result: any = await queueAdapter.removeFromQueueBatch(messages);
    // Should make 2 batch calls (10 + 5 messages)
    expect(result.successful.length).toBe(20); // 10 from first batch + 10 from second mock response
  });

  it('should handle batch delete errors', async () => {
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
    sqsMock.on(DeleteMessageBatchCommand).rejects(errMsg);
    const queueAdapter = new SqsQueueAdapter(Logger, { skipQueueExistsCheck: true });
    const messages: Message[] = [
      {
        MessageId: 'msg-1',
        ReceiptHandle: 'receipt-1',
        Body: JSON.stringify({ event: 'loading' }),
      },
    ];
    const result: any = await queueAdapter.removeFromQueueBatch(messages);
    expect(result.failed.length).toBe(1);
    expect(result.failed[0].Code).toBe('BatchError');
  });
});
