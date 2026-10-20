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

  const insertedArgs = (): any => insertSpy.calls.mostRecent().args[0];

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

  describe('timestamp encoding (ClickHouse >= 26.8 bare-integer regression)', () => {
    // Regression guard: a bare epoch-ms integer is read as Unix seconds from
    // ClickHouse 26.8, overflowing DateTime64(3) to 9999-12-31. The adapter must
    // instead send an explicit ISO-8601 UTC string and request best_effort
    // datetime parsing so both 24.x and 26.x store the same instant.
    const MS = Date.UTC(2026, 9, 19, 10, 0, 0, 123); // 2026-10-19T10:00:00.123Z

    it('putItem sends timestamp as an ISO-8601 UTC string', async () => {
      await adapter.putItem({
        tableName: 'test_table_1',
        data: { event: 'playing', sessionId: '123', timestamp: MS },
      });
      expect(insertedRow().timestamp).toEqual('2026-10-19T10:00:00.123Z');
    });

    it('putItem passes date_time_input_format: best_effort to insert()', async () => {
      await adapter.putItem({
        tableName: 'test_table_1',
        data: { event: 'playing', sessionId: '123', timestamp: MS },
      });
      expect(insertedArgs().clickhouse_settings).toEqual({
        date_time_input_format: 'best_effort',
      });
    });

    it('putItems sends timestamp as an ISO-8601 UTC string', async () => {
      await adapter.putItems({
        tableName: 'test_table_1',
        data: [{ event: 'playing', sessionId: '123', timestamp: MS }],
      });
      expect(insertedRow().timestamp).toEqual('2026-10-19T10:00:00.123Z');
    });

    it('putItems passes date_time_input_format: best_effort to insert()', async () => {
      await adapter.putItems({
        tableName: 'test_table_1',
        data: [{ event: 'playing', sessionId: '123', timestamp: MS }],
      });
      expect(insertedArgs().clickhouse_settings).toEqual({
        date_time_input_format: 'best_effort',
      });
    });
  });

  describe('geo/domain fields (eventsink 0.6.0)', () => {
    it('putItem maps top-level country, city and domain to row values', async () => {
      await adapter.putItem({
        tableName: 'test_table_1',
        data: {
          event: 'metadata',
          sessionId: '123',
          timestamp: 0,
          country: 'SE',
          city: 'Stockholm',
          domain: 'example.com',
        },
      });
      const row = insertedRow();
      expect(row.country).toEqual('SE');
      expect(row.city).toEqual('Stockholm');
      expect(row.domain).toEqual('example.com');
    });

    it('putItem defaults missing country/city/domain to empty string', async () => {
      await adapter.putItem({
        tableName: 'test_table_1',
        data: {
          event: 'playing',
          sessionId: '123',
          timestamp: 0,
        },
      });
      const row = insertedRow();
      expect(row.country).toEqual('');
      expect(row.city).toEqual('');
      expect(row.domain).toEqual('');
    });

    it('putItems maps top-level country, city and domain to row values', async () => {
      await adapter.putItems({
        tableName: 'test_table_1',
        data: [
          {
            event: 'metadata',
            sessionId: '123',
            timestamp: 0,
            country: 'NO',
            city: 'Oslo',
            domain: 'player.example.org',
          },
        ],
      });
      const row = insertedRow();
      expect(row.country).toEqual('NO');
      expect(row.city).toEqual('Oslo');
      expect(row.domain).toEqual('player.example.org');
    });

    it('putItems defaults missing country/city/domain to empty string', async () => {
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
      expect(row.country).toEqual('');
      expect(row.city).toEqual('');
      expect(row.domain).toEqual('');
    });
  });

  describe('geo/domain column migration (existing tables)', () => {
    it('issues ADD COLUMN IF NOT EXISTS for country, city and domain when the table already exists', async () => {
      const querySpy = jasmine
        .createSpy('query')
        .and.callFake((_args: any) => ({
          // tableExists lookup resolves to one row => table already present.
          json: async () => [{ 1: 1 }],
        }));
      adapter.dbClient = { query: querySpy };

      const result = await adapter.tableExists('existing_events');
      expect(result).toBeTrue();

      const migrationQueries = querySpy.calls
        .allArgs()
        .map((args) => args[0].query as string)
        .filter((q) => q.includes('ALTER TABLE'));

      expect(migrationQueries.length).toEqual(3);
      expect(migrationQueries[0]).toContain(
        "ADD COLUMN IF NOT EXISTS country String DEFAULT ''"
      );
      expect(migrationQueries[1]).toContain(
        "ADD COLUMN IF NOT EXISTS city String DEFAULT ''"
      );
      expect(migrationQueries[2]).toContain(
        "ADD COLUMN IF NOT EXISTS domain String DEFAULT ''"
      );
      for (const q of migrationQueries) {
        expect(q).toContain('ALTER TABLE existing_events');
      }
    });

    it('does not run the migration for an injection table name', async () => {
      const querySpy = jasmine.createSpy('query');
      adapter.dbClient = { query: querySpy };
      await expectAsync(
        adapter.tableExists("events'; DROP TABLE events; --")
      ).toBeRejectedWithError(/Invalid ClickHouse table name/);
      expect(querySpy).not.toHaveBeenCalled();
    });
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
