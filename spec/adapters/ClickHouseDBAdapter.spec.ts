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

  describe('table-name validation (SQL injection hardening)', () => {
    // The adapter interpolates tableName directly into DDL/DML strings
    // (CREATE TABLE, SELECT ... FROM system.tables), so an allowlist is
    // the only defence. These tests pin the guard in place.

    const validNames = [
      'events',
      'player_events',
      '_private',
      'EventsV2',
      'a',
      'A'.repeat(128),
    ];
    const invalidNames = [
      '',
      '1events',                              // starts with digit
      'events table',                         // whitespace
      'events; DROP TABLE events; --',        // classic injection
      "events' OR '1'='1",                    // quote-break
      'events`',                              // backtick
      'events"',                              // double quote
      'events--',                             // hyphen
      'events\n',                             // newline
      'A'.repeat(129),                        // too long
    ];

    for (const name of validNames) {
      it(`accepts valid table name: ${JSON.stringify(name).slice(0, 30)}`, async () => {
        await expectAsync(
          adapter.putItem({
            tableName: name,
            data: { event: 'playing', sessionId: 's', timestamp: 0 },
          })
        ).toBeResolved();
      });
    }

    for (const name of invalidNames) {
      it(`rejects invalid table name: ${JSON.stringify(name).slice(0, 40)}`, async () => {
        await expectAsync(
          adapter.putItem({
            tableName: name,
            data: { event: 'playing', sessionId: 's', timestamp: 0 },
          })
        ).toBeRejectedWithError(/Invalid ClickHouse table name/);
      });
    }

    it('putItems rejects an injection payload before issuing any insert', async () => {
      await expectAsync(
        adapter.putItems({
          tableName: 'events; DROP TABLE events; --',
          data: [{ event: 'playing', sessionId: 's', timestamp: 0 }],
        })
      ).toBeRejectedWithError(/Invalid ClickHouse table name/);
      expect(insertSpy).not.toHaveBeenCalled();
    });

    it('tableExists rejects an injection payload before issuing any query', async () => {
      const querySpy = jasmine.createSpy('query');
      adapter.dbClient = { query: querySpy };
      await expectAsync(
        adapter.tableExists("events' OR '1'='1")
      ).toBeRejectedWithError(/Invalid ClickHouse table name/);
      expect(querySpy).not.toHaveBeenCalled();
    });
  });
});
