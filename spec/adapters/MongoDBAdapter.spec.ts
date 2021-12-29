import { MongoMemoryServer } from 'mongodb-memory-server';
import { MongoDBAdapter } from '../../adapters/db/MongoDBAdapter';
import Logger from '../../util/logger';

describe('Mongo DB Adapter', () => {
  let adapter: MongoDBAdapter;
  beforeAll(async () => {
    const instance = await MongoMemoryServer.create();
    const uri = instance.getUri();
    (global as any).__MONGOINSTANCE = instance;
    process.env.MONGODB_URI = uri.slice(0, uri.lastIndexOf('/'));
    adapter = new MongoDBAdapter(Logger);
  });

  beforeEach(async () => {
    const collections = await adapter.getTableNames();
    for (const collection of collections) {
      const c = await adapter.dbClient.db().collection(collection);
      await c.drop();
    }
  });

  afterAll(async () => {
    const collections = await adapter.getTableNames();
    for (const collection of collections) {
      const c = await adapter.dbClient.db().collection(collection);
      await c.drop();
    }
    await adapter.dbClient.close();
  });

  it('should return list of table names in database', async () => {
    const mockTables = ['test_table_1', 'test_table_2'];

    const result = await adapter.getTableNames();
    expect(result).toEqual([]);

    await adapter.createTable('test_table_1');
    await adapter.createTable('test_table_2');

    setTimeout(async () => {
      const result = await adapter.getTableNames();
      expect(result).toEqual(mockTables);
    }, 1000);
  });

  it('should create a table in database', async () => {
    const tableName = 'test_table_1';
    const result = await adapter.createTable(tableName);
    expect(result).toEqual('test_table_1');
  });

  it('should put item to database', async () => {
    const mockEvent = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
      host: 'mock.tenant.mock',
    };
    const result = await adapter.putItem({
      tableName: 'test_table_1',
      data: mockEvent,
    });
    expect(result).toBeTrue();
  });

  it('should get item from database', async () => {
    const mockId = '123-123-123-123';
    await adapter.putItem({
      tableName: 'test_table_1',
      data: {
        event: 'loading',
        timestamp: 0,
        playhead: 0,
        duration: 0,
        host: 'mock.tenant.mock',
        sessionId: mockId,
      },
    });
    const result = await adapter.getItem({
      tableName: "test_table_1",
      sessionId: mockId,
      timestamp: 0,
    });
    expect(result).toBeDefined();
    expect(result.sessionId).toEqual(mockId);
    expect(result.host).toEqual('mock.tenant.mock');
  });

  it('should delete item in database', async () => {
    const mockId = '123-123-123-123';
    await adapter.putItem({
      tableName: 'test_table_1',
      data: {
        event: 'loading',
        timestamp: 0,
        playhead: 0,
        duration: 0,
        host: 'mock.tenant.mock',
        sessionId: mockId,
      },
    });
    const result = await adapter.deleteItem({
      tableName: 'test_table_1',
      sessionId: mockId,
      timestamp: 0,
    });
    expect(result).toBeTrue();
  });

  it('should get items from db with a specific sessionId and convert them to valid event objects', async () => {
    const mockItems = [
      {
        event: 'playing',
        sessionId: '123-214-234',
        timestamp: 1640191099,
        playhead: 1,
        duration: 0,
      },
      {
        event: 'playing',
        sessionId: '123-214-234',
        timestamp: 1640193099,
        playhead: 3,
        duration: 0,
      },
      {
        event: 'paused',
        sessionId: '123-214-234',
        timestamp: 1640192099,
        playhead: 2,
        duration: 0,
      },
    ];
    // for each mock items, put them to db
    for (const mockItem of mockItems) {
      await adapter.putItem({
        tableName: 'test_table_1',
        data: mockItem,
      });
    }
    const result = await adapter.getItemsBySession({
      tableName: 'test_table_1',
      sessionId: '123-214-234',
    });
    // remove the mongodb id to be able to compare by equality
    const cleanedResult = result.map((item) => {
      delete item._id;
      return item;
    });
    expect(cleanedResult).toEqual([
      {
        event: 'playing',
        sessionId: '123-214-234',
        timestamp: 1640191099,
        duration: 0,
        playhead: 1,
      },
      {
        event: 'playing',
        sessionId: '123-214-234',
        timestamp: 1640193099,
        duration: 0,
        playhead: 3,
      },
      {
        event: 'paused',
        sessionId: '123-214-234',
        timestamp: 1640192099,
        duration: 0,
        playhead: 2,
      },
    ]);
  });
});
