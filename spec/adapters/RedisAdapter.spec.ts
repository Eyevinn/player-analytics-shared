import Logger from '../../util/logger';
import { RedisAdapter } from '../../adapters/queue/RedisAdapter';

Logger.silent = true;

describe('Redis Queue Adapter', () => {
  let adapter: RedisAdapter;
  let mockClient: {
    add: jasmine.Spy;
    get: jasmine.Spy;
    getStatus: jasmine.Spy;
  };

  beforeEach(() => {
    adapter = new RedisAdapter(Logger);

    // Replace the real client with a mock
    mockClient = {
      add: jasmine.createSpy('add'),
      get: jasmine.createSpy('get'),
      getStatus: jasmine.createSpy('getStatus'),
    };
    (adapter as any).client = mockClient;
  });

  describe('pushToQueue()', () => {
    it('should push event data to redis queue', async () => {
      const event = { event: 'playing', sessionId: 'sess-1', timestamp: 1000 };
      mockClient.add.and.returnValue(Promise.resolve({ id: 'job-1' }));

      const result = await adapter.pushToQueue(event);

      expect(mockClient.add).toHaveBeenCalledWith({ data: event });
      expect(result).toEqual({ id: 'job-1' });
    });

    it('should propagate errors from redis client', async () => {
      mockClient.add.and.returnValue(Promise.reject(new Error('Connection refused')));

      await expectAsync(adapter.pushToQueue({})).toBeRejectedWithError('Connection refused');
    });
  });

  describe('pullFromQueue()', () => {
    it('should return a job from the queue', async () => {
      const job = { id: 'job-1', data: { event: 'playing' } };
      mockClient.get.and.returnValue(Promise.resolve(job));

      const result = await adapter.pullFromQueue();

      expect(mockClient.get).toHaveBeenCalled();
      expect(result).toEqual(job);
    });

    it('should return empty object when queue is empty', async () => {
      mockClient.get.and.returnValue(Promise.resolve(null));

      const result = await adapter.pullFromQueue();

      expect(result).toEqual({});
    });
  });

  describe('removeFromQueue()', () => {
    it('should return true when job status is completed', async () => {
      mockClient.getStatus.and.returnValue(Promise.resolve('completed'));

      const result = await adapter.removeFromQueue({ id: 'job-1' });

      expect(mockClient.getStatus).toHaveBeenCalledWith('job-1');
      expect(result).toBe(true);
    });

    it('should return false when job status is not completed', async () => {
      mockClient.getStatus.and.returnValue(Promise.resolve('pending'));

      const result = await adapter.removeFromQueue({ id: 'job-1' });

      expect(result).toBe(false);
    });
  });

  describe('removeFromQueueBatch()', () => {
    it('should track successful and failed removals', async () => {
      mockClient.getStatus
        .and.returnValues(
          Promise.resolve('completed'),
          Promise.resolve('pending'),
          Promise.resolve('completed'),
        );

      const messages = [
        { id: 'job-1' },
        { id: 'job-2' },
        { id: 'job-3' },
      ];

      const result = await adapter.removeFromQueueBatch(messages) as any;

      expect(result.successful.length).toBe(2);
      expect(result.failed.length).toBe(1);
      expect(result.failed[0].id).toBe('job-2');
      expect(result.failed[0].reason).toBe('not completed');
    });

    it('should handle errors in batch removal', async () => {
      mockClient.getStatus
        .and.returnValues(
          Promise.resolve('completed'),
          Promise.reject(new Error('Redis timeout')),
        );

      const messages = [{ id: 'job-1' }, { id: 'job-2' }];
      const result = await adapter.removeFromQueueBatch(messages) as any;

      expect(result.successful.length).toBe(1);
      expect(result.failed.length).toBe(1);
      expect(result.failed[0].reason).toBe('Redis timeout');
    });
  });

  describe('getEventJSONsFromMessages()', () => {
    it('should return body unchanged (not yet implemented)', () => {
      spyOn(Logger, 'warn');
      const messages = [{ event: 'playing' }, { event: 'paused' }];

      const result = adapter.getEventJSONsFromMessages(messages);

      expect(result).toEqual(messages);
    });
  });
});
