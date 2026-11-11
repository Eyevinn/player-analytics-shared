import { createClient } from '@clickhouse/client';
import { AbstractDBAdapter, ErrorType, IGetItemInput, IGetItems, IHandleErrorOutput, IPutItemInput, IPutItemsInput } from '../../types/interfaces';
import winston from 'winston';

interface EventItem {
  event: string;
  sessionId: string;
  timestamp: number;
  playhead: number;
  duration: number;
  // Top-level geo/domain fields added by eventsink 0.6.0: country/city are
  // derived via geo-IP on metadata events, domain comes from the Origin header.
  // They are not present on every event, hence optional.
  country?: string;
  city?: string;
  domain?: string;
  payload: string;
}

export class ClickHouseDBAdapter implements AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: any;

  // ClickHouse unquoted identifier grammar: start with letter or underscore,
  // then letters/digits/underscores. Interpolated into DDL/DML strings in
  // this adapter, so an allowlist is the only defence against injection.
  private static readonly TABLE_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]{0,127}$/;

  private static validateTableName(name: unknown): asserts name is string {
    if (typeof name !== 'string' || !ClickHouseDBAdapter.TABLE_NAME_PATTERN.test(name)) {
      throw new Error(
        `Invalid ClickHouse table name: ${JSON.stringify(name)}. ` +
        `Must match ${ClickHouseDBAdapter.TABLE_NAME_PATTERN}.`
      );
    }
  }

  // The EPAS wire timestamp is epoch milliseconds (Date.now()). Sending it as a
  // bare JSON integer into a DateTime64(3) column is ambiguous: up to ClickHouse
  // 26.7 it was read as raw ticks (ms, correct), but from 26.8 a bare number is
  // read as Unix *seconds* (setting input_format_read_datetime_number_as_raw_value
  // defaults to 0), so millisecond values overflow to 9999-12-31. Sending an
  // explicit ISO-8601 UTC string plus date_time_input_format: 'best_effort' is
  // parsed identically on both 24.x and 26.x, keeping the stored instant stable
  // regardless of ClickHouse version. The trailing Z makes it timezone-independent.
  private static readonly INSERT_SETTINGS = {
    date_time_input_format: 'best_effort'
  } as const;

  private static toClickHouseDateTime(ms: number): string {
    return new Date(ms).toISOString();
  }

  constructor(logger: winston.Logger) {
    this.logger = logger;
    this.dbClient = createClient({
      url: process.env.CLICKHOUSE_URL
    });
  }

  async tableExists(name: string): Promise<boolean> {
    ClickHouseDBAdapter.validateTableName(name);
    try {
      const query = `SELECT 1 FROM system.tables WHERE database = currentDatabase() AND name = '${name}'`;
      const resultSet = await this.dbClient.query({
        query,
        format: 'JSONEachRow'
      });
      
      const rows = await resultSet.json();
      if (rows.length > 0) {
        // Table already exists. CREATE TABLE IF NOT EXISTS would not add any
        // newly introduced columns, so run the geo/domain migration to bring
        // pre-existing customer tables up to date without data loss.
        await this.ensureGeoDomainColumns(name);
        return true;
      } else {
        // Create table if it does not exists
        const createTableQuery = `
        CREATE TABLE IF NOT EXISTS ${name} (
          event String,
          sessionId String,
          timestamp DateTime64(3),
          playhead Float64,
          duration Float64,
          live Boolean,
          contentId String,
          userId String,
          deviceId String,
          deviceModel String,
          deviceType String,
          country String DEFAULT '',
          city String DEFAULT '',
          domain String DEFAULT '',
          payload String, /* Stored as JSON string */
          
          /* Add derived columns for better query performance */
          event_date Date DEFAULT toDate(timestamp),
          event_hour DateTime DEFAULT toStartOfHour(timestamp)
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (sessionId, timestamp)
      `;
      
      await this.dbClient.query({
        query: createTableQuery
      });
      
      this.logger.info(`Table '${name}' created successfully.`);
      return true;
      }
    } catch (err) {
      this.logger.error(`Error checking if table '${name}' exists:`);
      this.logger.error(err);
      return false;
    }
  }

  /**
   * Ensure the geo/domain columns introduced by eventsink 0.6.0 exist on an
   * already-created table. CREATE TABLE IF NOT EXISTS does not alter existing
   * tables, so ADD COLUMN IF NOT EXISTS is issued per column; it is a no-op
   * when the column is already present.
   */
  private async ensureGeoDomainColumns(name: string): Promise<void> {
    ClickHouseDBAdapter.validateTableName(name);
    for (const column of ['country', 'city', 'domain']) {
      await this.dbClient.query({
        query: `ALTER TABLE ${name} ADD COLUMN IF NOT EXISTS ${column} String DEFAULT ''`
      });
    }
  }

  async putItem(params: IPutItemInput): Promise<boolean> {
    const tableName = params.tableName;
    ClickHouseDBAdapter.validateTableName(tableName);
    const item = params.data as EventItem;

    // Prepare the item for insertion
    this.logger.debug(item);
    
    // Convert payload to JSON string if it's an object
    const payload = typeof item.payload === 'object' 
      ? JSON.stringify(item.payload) 
      : item.payload || '';
    
    // Prepare the data for insertion
    let parsedPayload = {};
    if (payload) {
      try {
        parsedPayload = JSON.parse(payload);
      } catch (_error) {
        this.logger.warn('Payload not json, skipping parsing');
      }
    }
    const data = [{
      event: item.event,
      sessionId: item.sessionId,
      timestamp: ClickHouseDBAdapter.toClickHouseDateTime(item.timestamp),
      playhead: item.playhead ?? -1,
      duration: item.duration ?? -1,
      live: parsedPayload['live'] || false,
      contentId: parsedPayload['contentId'] || '',
      userId: parsedPayload['userId'] || '',
      deviceId: parsedPayload['deviceId'] || '',
      deviceModel: parsedPayload['deviceModel'] || '',
      deviceType: parsedPayload['deviceType'] || '',
      country: item.country ?? '',
      city: item.city ?? '',
      domain: item.domain ?? '',
      payload
    }];
    
    // Insert the data
    await this.dbClient.insert({
      table: tableName,
      values: data,
      format: 'JSONEachRow',
      clickhouse_settings: ClickHouseDBAdapter.INSERT_SETTINGS
    });

    this.logger.debug(`Successfully inserted item into ${tableName}`);
    return true;
  }

  async putItems(params: IPutItemsInput): Promise<boolean> {
    const tableName = params.tableName;
    ClickHouseDBAdapter.validateTableName(tableName);
    const items = params.data as EventItem[];

    if (!items || items.length === 0) {
      this.logger.warn('No items provided for batch insert');
      return true;
    }

    this.logger.debug(`Batch inserting ${items.length} items into ${tableName}`);
    
    // Prepare all items for insertion
    const data = items.map(item => {
      // Convert payload to JSON string if it's an object
      const payload = typeof item.payload === 'object' 
        ? JSON.stringify(item.payload) 
        : item.payload || '';
      
      // Prepare the data for insertion
      let parsedPayload = {};
      if (payload) {
        try {
          parsedPayload = JSON.parse(payload);
        } catch (_error) {
          this.logger.warn('Payload not json, skipping parsing');
        }
      }

      return {
        event: item.event,
        sessionId: item.sessionId,
        timestamp: ClickHouseDBAdapter.toClickHouseDateTime(item.timestamp),
        playhead: item.playhead ?? -1,
        duration: item.duration ?? -1,
        live: parsedPayload['live'] || false,
        contentId: parsedPayload['contentId'] || '',
        userId: parsedPayload['userId'] || '',
        deviceId: parsedPayload['deviceId'] || '',
        deviceModel: parsedPayload['deviceModel'] || '',
        deviceType: parsedPayload['deviceType'] || '',
        country: item.country ?? '',
        city: item.city ?? '',
        domain: item.domain ?? '',
        payload
      };
    });
    
    // Insert all data in a single batch operation
    await this.dbClient.insert({
      table: tableName,
      values: data,
      format: 'JSONEachRow',
      clickhouse_settings: ClickHouseDBAdapter.INSERT_SETTINGS
    });

    this.logger.debug(`Successfully batch inserted ${items.length} items into ${tableName}`);
    return true;
  }

  async getItem(_params: IGetItemInput): Promise<any> {
    throw new Error('Method not implemented.');
  }

  async deleteItem(_params: IGetItemInput): Promise<boolean> {
    throw new Error('Method not implemented.');
  }

  async getItemsBySession(_params: IGetItems): Promise<any[]> {
    throw new Error('Method not implemented.');
  }

  handleError(errorObject: any): IHandleErrorOutput {
    this.logger.error(errorObject);
    const errorOutput: IHandleErrorOutput = {
      errorType: ErrorType.ABORT,
      error: errorObject,
    };
    return errorOutput;
  }
}
