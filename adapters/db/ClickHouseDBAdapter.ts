import { createClient } from '@clickhouse/client';
import { AbstractDBAdapter, ErrorType, IGetItemInput, IGetItems, IHandleErrorOutput, IPutItemInput } from '../../types/interfaces';
import winston from 'winston';

interface EventItem {
  event: string;
  sessionId: string;
  timestamp: number;
  playhead: number;
  duration: number;
  payload: string;
}

export class ClickHouseDBAdapter implements AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: any;

  constructor(logger: winston.Logger) {
    this.logger = logger;
    this.dbClient = createClient({
      url: process.env.CLICKHOUSE_URL
    });
  }

  async tableExists(name: string): Promise<boolean> {
    try {
      const query = `SELECT 1 FROM system.tables WHERE database = currentDatabase() AND name = '${name}'`;
      const resultSet = await this.dbClient.query({
        query,
        format: 'JSONEachRow'
      });
      
      const rows = await resultSet.json();
      if (rows.length > 0) {
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

  async putItem(params: IPutItemInput): Promise<boolean> {
    const tableName = params.tableName;
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
      } catch (error) {
        this.logger.warn('Payload not json, skipping parsing');
      }
    }
    const data = [{
      event: item.event,
      sessionId: item.sessionId,
      timestamp: item.timestamp,
      playhead: item.playhead || -1,
      duration: item.duration || -1,
      live: parsedPayload['live'] || false,
      contentId: parsedPayload['contentId'] || '',
      userId: parsedPayload['userId'] || '',
      deviceId: parsedPayload['deviceId'] || '',
      deviceModel: parsedPayload['deviceModel'] || '',
      deviceType: parsedPayload['deviceType'] || '',
      payload
    }];
    
    // Insert the data
    await this.dbClient.insert({
      table: tableName,
      values: data,
      format: 'JSONEachRow'
    });
    
    this.logger.debug(`Successfully inserted item into ${tableName}`);
    return true;
  }

  async getItem(params: IGetItemInput): Promise<any> {
    throw new Error('Method not implemented.');
  }

  async deleteItem(params: IGetItemInput): Promise<boolean> {
    throw new Error('Method not implemented.');
  }

  async getItemsBySession(params: IGetItems): Promise<any[]> {
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
