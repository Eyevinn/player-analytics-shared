import Logger from '../../util/logger';
import { BeanstalkdAdapter } from '../../adapters/queue/BeanstalkdAdapter';

Logger.silent = true;

describe('Beanstalkd Queue Adapter', () => {
  let adapter: BeanstalkdAdapter;
  let mockClient: {
    connect: jasmine.Spy;
    put: jasmine.Spy;
    reserveWithTimeout: jasmine.Spy;
    delete: jasmine.Spy;
    isConnected: boolean;
  };

  beforeEach(() => {
    adapter = new BeanstalkdAdapter(Logger);

    // Replace the real client with a mock
    mockClient = {
      connect: jasmine.createSpy('connect').and.returnValue(Promise.resolve()),
      put: jasmine.createSpy('put'),
      reserveWithTimeout: jasmine.createSpy('reserveWithTimeout'),
      delete: jasmine.createSpy('delete'),
      isConnected: false,
    };
    (adapter as any).client = mockClient;
  });

  describe('pushToQueue()', () => {
    it('should connect and push event data', async () => {
      const event = { event: 'playing', sessionId: 'sess-1' };
      mockClient.put.and.returnValue(Promise.resolve({ id: 'job-1' }));

      const result = await adapter.pushToQueue(event);

      expect(mockClient.connect).toHaveBeenCalled();
      expect(mockClient.put).toHaveBeenCalledWith(event);
      expect(result).toEqual({ id: 'job-1' });
    });

    it('should skip connect if already connected', async () => {
      mockClient.isConnected = true;
      mockClient.put.and.returnValue(Promise.resolve({ id: 'job-1' }));

      await adapter.pushToQueue({});

      expect(mockClient.connect).not.toHaveBeenCalled();
      expect(mockClient.put).toHaveBeenCalled();
    });

    it('should propagate connection errors', async () => {
      mockClient.connect.and.returnValue(Promise.reject(new Error('ECONNREFUSED')));

      await expectAsync(adapter.pushToQueue({})).toBeRejectedWithError('ECONNREFUSED');
    });
  });

  describe('pullFromQueue()', () => {
    it('should connect and reserve a job with timeout', async () => {
      const job = { id: 'job-1', body: '{"event":"playing"}' };
      mockClient.reserveWithTimeout.and.returnValue(Promise.resolve(job));

      const result = await adapter.pullFromQueue();

      expect(mockClient.connect).toHaveBeenCalled();
      expect(mockClient.reserveWithTimeout).toHaveBeenCalledWith(1);
      expect(result).toEqual(job);
    });

    it('should return empty object when no job available', async () => {
      mockClient.reserveWithTimeout.and.returnValue(Promise.resolve(null));

      const result = await adapter.pullFromQueue();

      expect(result).toEqual({});
    });
  });

  describe('removeFromQueue()', () => {
    it('should connect and delete job by id', async () => {
      mockClient.delete.and.returnValue(Promise.resolve(true));

      const result = await adapter.removeFromQueue({ id: 'job-1' });

      expect(mockClient.connect).toHaveBeenCalled();
      expect(mockClient.delete).toHaveBeenCalledWith('job-1');
      expect(result).toBe(true);
    });

    it('should return false when delete fails', async () => {
      mockClient.delete.and.returnValue(Promise.resolve(false));

      const result = await adapter.removeFromQueue({ id: 'job-1' });

      expect(result).toBe(false);
    });
  });

  describe('removeFromQueueBatch()', () => {
    it('should track successful and failed removals', async () => {
      mockClient.isConnected = true;
      mockClient.delete
        .and.returnValues(
          Promise.resolve(true),
          Promise.resolve(false),
          Promise.resolve(true),
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
      expect(result.failed[0].reason).toBe('delete failed');
    });

    it('should handle errors in batch removal', async () => {
      mockClient.isConnected = true;
      mockClient.delete
        .and.returnValues(
          Promise.resolve(true),
          Promise.reject(new Error('Connection lost')),
        );

      const messages = [{ id: 'job-1' }, { id: 'job-2' }];
      const result = await adapter.removeFromQueueBatch(messages) as any;

      expect(result.successful.length).toBe(1);
      expect(result.failed.length).toBe(1);
      expect(result.failed[0].reason).toBe('Connection lost');
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
