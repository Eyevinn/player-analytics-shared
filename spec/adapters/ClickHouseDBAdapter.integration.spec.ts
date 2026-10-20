import { ClickHouseDBAdapter } from '../../adapters/db/ClickHouseDBAdapter';
import Logger from '../../util/logger';

/**
 * Integration test for ClickHouseDBAdapter against a real ClickHouse server.
 *
 * It is skipped by default (so `npm test` stays hermetic for local/unit runs)
 * and only executes when RUN_CLICKHOUSE_INTEGRATION=true and CLICKHOUSE_URL
 * point at a live server. CI runs it twice via a service-container matrix over
 * clickhouse/clickhouse-server:24.12 and :26.8 (see .github/workflows/build-test.yml)
 * to prove the timestamp round-trips identically before and after the 26.8
 * bare-integer-as-seconds change.
 *
 * Acceptance: SELECT toUnixTimestamp64Milli(timestamp) must equal the inserted
 * epoch-ms value exactly, and the derived event_date / event_hour must match,
 * on both ClickHouse versions.
 */
const RUN = process.env.RUN_CLICKHOUSE_INTEGRATION === 'true';

(RUN ? describe : xdescribe)('ClickHouse DB Adapter (integration)', () => {
  const TABLE = 'events_integration';
  // A millisecond-precision instant whose ms value would overflow DateTime64(3)
  // if (mis)read as Unix seconds.
  const MS = Date.UTC(2026, 9, 19, 10, 0, 0, 123); // 2026-10-19T10:00:00.123Z
  let adapter: ClickHouseDBAdapter;

  const queryRow = async (sessionId: string): Promise<any> => {
    const resultSet = await adapter.dbClient.query({
      query:
        `SELECT toUnixTimestamp64Milli(timestamp) AS ms, ` +
        `toString(event_date) AS event_date, ` +
        `toString(event_hour) AS event_hour ` +
        `FROM ${TABLE} WHERE sessionId = {sid:String}`,
      query_params: { sid: sessionId },
      format: 'JSONEachRow',
    });
    const rows = await resultSet.json();
    return rows[0];
  };

  beforeAll(async () => {
    adapter = new ClickHouseDBAdapter(Logger);
    const created = await adapter.tableExists(TABLE);
    expect(created).toBeTrue();
  });

  afterAll(async () => {
    if (adapter && adapter.dbClient && adapter.dbClient.close) {
      await adapter.dbClient.close();
    }
  });

  it('putItem round-trips the millisecond timestamp exactly', async () => {
    await adapter.putItem({
      tableName: TABLE,
      data: {
        event: 'playing',
        sessionId: 'single',
        timestamp: MS,
        playhead: 1,
        duration: 2,
      },
    });

    const row = await queryRow('single');
    // toUnixTimestamp64Milli returns a string over the HTTP/JSON interface.
    expect(Number(row.ms)).toEqual(MS);
    expect(row.event_date).toEqual('2026-10-19');
    expect(row.event_hour).toEqual('2026-10-19 10:00:00');
  });

  it('putItems round-trips the millisecond timestamp exactly', async () => {
    await adapter.putItems({
      tableName: TABLE,
      data: [
        {
          event: 'playing',
          sessionId: 'batch',
          timestamp: MS,
          playhead: 3,
          duration: 4,
        },
      ],
    });

    const row = await queryRow('batch');
    expect(Number(row.ms)).toEqual(MS);
    expect(row.event_date).toEqual('2026-10-19');
    expect(row.event_hour).toEqual('2026-10-19 10:00:00');
  });
});
