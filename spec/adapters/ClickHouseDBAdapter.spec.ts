import Logger from '../../util/logger';

// Mock @clickhouse/client module before importing the adapter
const mockClient = {
  query: jasmine.createSpy('query'),
  insert: jasmine.createSpy('insert'),
  close: jasmine.createSpy('close'),
};

const mockCreateClient = jasmine.createSpy('createClient').and.returnValue(mockClient);

// Mock the module
(require as any).cache[require.resolve('@clickhouse/client')] = {
  exports: {
    createClient: mockCreateClient,
  },
};

import { ClickHouseDBAdapter } from '../../adapters/db/ClickHouseDBAdapter';

describe('ClickHouse DB Adapter', () => {
  let adapter: ClickHouseDBAdapter;

  beforeEach(() => {
    process.env.CLICKHOUSE_URL = 'http://localhost:8123';
    mockClient.query.calls.reset();
    mockClient.insert.calls.reset();
    mockClient.close.calls.reset();
    mockCreateClient.calls.reset();
    adapter = new ClickHouseDBAdapter(Logger);
  });

  afterEach(() => {
    delete process.env.CLICKHOUSE_URL;
  });

  it('should return true if table exists in database', async () => {
    const mockResultSet = {
      json: jasmine.createSpy('json').and.returnValue(Promise.resolve([{ '1': 1 }])),
    };
    mockClient.query.and.returnValue(Promise.resolve(mockResultSet));

    const result = await adapter.tableExists('test_table');

    expect(result).toBeTrue();
    expect(mockClient.query).toHaveBeenCalledWith({
      query: "SELECT 1 FROM system.tables WHERE database = currentDatabase() AND name = 'test_table'",
      format: 'JSONEachRow',
    });
  });

  it('should return false if table name contains SQL injection attempt', async () => {
    const result = await adapter.tableExists("test'; DROP TABLE users; --");

    expect(result).toBeFalse();
    expect(mockClient.query).not.toHaveBeenCalled();
  });

  it('should create table if it does not exist', async () => {
    const mockResultSet = {
      json: jasmine.createSpy('json').and.returnValue(Promise.resolve([])),
    };
    mockClient.query.and.returnValue(Promise.resolve(mockResultSet));

    const result = await adapter.tableExists('test_table');

    expect(result).toBeTrue();
    expect(mockClient.query).toHaveBeenCalledTimes(2);
    // First call checks if table exists
    expect(mockClient.query.calls.argsFor(0)[0].query).toContain('SELECT 1 FROM system.tables');
    // Second call creates the table
    expect(mockClient.query.calls.argsFor(1)[0].query).toContain('CREATE TABLE IF NOT EXISTS test_table');
  });

  it('should return false when error occurs checking table existence', async () => {
    const mockError = new Error('Connection failed');
    mockClient.query.and.callFake(() => Promise.reject(mockError));

    const result = await adapter.tableExists('test_table');

    expect(result).toBeFalse();
  });

  it('should put item to database with correct format', async () => {
    mockClient.insert.and.returnValue(Promise.resolve());

    const mockEvent = {
      event: 'loading',
      sessionId: 'session-123',
      timestamp: 1640191099000,
      playhead: 5.5,
      duration: 120,
      payload: JSON.stringify({
        live: false,
        contentId: 'video-123',
        userId: 'user-456',
        deviceId: 'device-789',
        deviceModel: 'iPhone 12',
        deviceType: 'mobile',
      }),
    };

    const result = await adapter.putItem({
      tableName: 'test_table',
      data: mockEvent,
    });

    expect(result).toBeTrue();
    expect(mockClient.insert).toHaveBeenCalledWith({
      table: 'test_table',
      values: [{
        event: 'loading',
        sessionId: 'session-123',
        timestamp: 1640191099000,
        playhead: 5.5,
        duration: 120,
        live: false,
        contentId: 'video-123',
        userId: 'user-456',
        deviceId: 'device-789',
        deviceModel: 'iPhone 12',
        deviceType: 'mobile',
        payload: mockEvent.payload,
      }],
      format: 'JSONEachRow',
    });
  });

  it('should put item with default values when optional fields are missing', async () => {
    mockClient.insert.and.returnValue(Promise.resolve());

    const mockEvent = {
      event: 'playing',
      sessionId: 'session-456',
      timestamp: 1640191099000,
      playhead: null,
      duration: null,
      payload: '',
    };

    const result = await adapter.putItem({
      tableName: 'test_table',
      data: mockEvent,
    });

    expect(result).toBeTrue();
    expect(mockClient.insert).toHaveBeenCalledWith({
      table: 'test_table',
      values: [{
        event: 'playing',
        sessionId: 'session-456',
        timestamp: 1640191099000,
        playhead: -1,
        duration: -1,
        live: false,
        contentId: '',
        userId: '',
        deviceId: '',
        deviceModel: '',
        deviceType: '',
        payload: '',
      }],
      format: 'JSONEachRow',
    });
  });

  it('should return false when putItem fails', async () => {
    const mockError = new Error('Insert failed');
    mockClient.insert.and.callFake(() => Promise.reject(mockError));

    const mockEvent = {
      event: 'loading',
      sessionId: 'session-123',
      timestamp: 1640191099000,
      playhead: 0,
      duration: 0,
      payload: '',
    };

    const result = await adapter.putItem({
      tableName: 'test_table',
      data: mockEvent,
    });

    expect(result).toBeFalse();
  });

  it('should batch insert multiple items to database', async () => {
    mockClient.insert.and.returnValue(Promise.resolve());

    const mockEvents = [
      {
        event: 'loading',
        sessionId: 'session-123',
        timestamp: 1640191099000,
        playhead: 0,
        duration: 120,
        payload: '{"contentId":"video-1"}',
      },
      {
        event: 'playing',
        sessionId: 'session-123',
        timestamp: 1640191100000,
        playhead: 1,
        duration: 120,
        payload: '{"contentId":"video-1"}',
      },
      {
        event: 'paused',
        sessionId: 'session-123',
        timestamp: 1640191105000,
        playhead: 6,
        duration: 120,
        payload: '{"contentId":"video-1"}',
      },
    ];

    const result = await adapter.putItems({
      tableName: 'test_table',
      data: mockEvents,
    });

    expect(result).toBeTrue();
    expect(mockClient.insert).toHaveBeenCalledTimes(1);
    expect(mockClient.insert.calls.argsFor(0)[0].values.length).toBe(3);
    expect(mockClient.insert.calls.argsFor(0)[0].table).toBe('test_table');
  });

  it('should return true when putItems is called with empty array', async () => {
    const result = await adapter.putItems({
      tableName: 'test_table',
      data: [],
    });

    expect(result).toBeTrue();
    expect(mockClient.insert).not.toHaveBeenCalled();
  });

  it('should return false when putItems fails', async () => {
    const mockError = new Error('Batch insert failed');
    mockClient.insert.and.callFake(() => Promise.reject(mockError));

    const mockEvents = [
      {
        event: 'loading',
        sessionId: 'session-123',
        timestamp: 1640191099000,
        playhead: 0,
        duration: 0,
        payload: '',
      },
    ];

    const result = await adapter.putItems({
      tableName: 'test_table',
      data: mockEvents,
    });

    expect(result).toBeFalse();
  });

  it('should throw "not implemented" error when getItem is called', async () => {
    await expectAsync(
      adapter.getItem({
        tableName: 'test_table',
        sessionId: 'session-123',
        timestamp: 1640191099000,
      })
    ).toBeRejectedWithError('Method not implemented.');
  });

  it('should throw "not implemented" error when deleteItem is called', async () => {
    await expectAsync(
      adapter.deleteItem({
        tableName: 'test_table',
        sessionId: 'session-123',
        timestamp: 1640191099000,
      })
    ).toBeRejectedWithError('Method not implemented.');
  });

  it('should throw "not implemented" error when getItemsBySession is called', async () => {
    await expectAsync(
      adapter.getItemsBySession({
        tableName: 'test_table',
        sessionId: 'session-123',
      })
    ).toBeRejectedWithError('Method not implemented.');
  });
});
