import { ClickHouseDBAdapter } from '../../adapters/db/ClickHouseDBAdapter';
import Logger from '../../util/logger';

describe('ClickHouse DB Adapter', () => {
  let adapter: ClickHouseDBAdapter;
  let insertSpy: jasmine.Spy;

  beforeEach(() => {
    adapter = new ClickHouseDBAdapter(Logger);
    // Replace the real ClickHouse client with a mock that captures inserts.
    insertSpy = jasmine.createSpy('insert').and.resolveTo(undefined);
    adapter.dbClient = { insert: insertSpy };
  });

  const insertedRow = (): any => {
    const call = insertSpy.calls.mostRecent();
    return call.args[0].values[0];
  };

  it('putItem should preserve playhead: 0 and duration: 0 (not -1)', async () => {
    await adapter.putItem({
      tableName: 'test_table_1',
      data: {
        event: 'playing',
        sessionId: '123',
        timestamp: 0,
        playhead: 0,
        duration: 0,
      },
    });
    const row = insertedRow();
    expect(row.playhead).toEqual(0);
    expect(row.duration).toEqual(0);
  });

  it('putItem should map missing playhead/duration to -1', async () => {
    await adapter.putItem({
      tableName: 'test_table_1',
      data: {
        event: 'playing',
        sessionId: '123',
        timestamp: 0,
      },
    });
    const row = insertedRow();
    expect(row.playhead).toEqual(-1);
    expect(row.duration).toEqual(-1);
  });

  it('putItems should preserve playhead: 0 and duration: 0 (not -1)', async () => {
    await adapter.putItems({
      tableName: 'test_table_1',
      data: [
        {
          event: 'playing',
          sessionId: '123',
          timestamp: 0,
          playhead: 0,
          duration: 0,
        },
      ],
    });
    const row = insertedRow();
    expect(row.playhead).toEqual(0);
    expect(row.duration).toEqual(0);
  });

  it('putItems should map missing playhead/duration to -1', async () => {
    await adapter.putItems({
      tableName: 'test_table_1',
      data: [
        {
          event: 'playing',
          sessionId: '123',
          timestamp: 0,
        },
      ],
    });
    const row = insertedRow();
    expect(row.playhead).toEqual(-1);
    expect(row.duration).toEqual(-1);
  });
});
