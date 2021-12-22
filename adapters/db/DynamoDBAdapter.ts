import {
  CreateTableCommand,
  ListTablesCommand,
  PutItemCommand,
  DynamoDBClient,
  GetItemCommand,
  DeleteItemCommand,
  AttributeValue,
  QueryCommand,
  QueryCommandOutput,
} from '@aws-sdk/client-dynamodb';
import winston from 'winston';
import {
  AbstractDBAdapter,
  IDDBGetItemInput,
  IDDBPutItemInput,
  IHandleErrorOutput,
  ErrorType,
  EventItem,
} from '../../types/interfaces';
import { v4 as uuidv4 } from 'uuid';

interface ITableItem {
  [key: string]: AttributeValue;
}

export class DynamoDBAdapter implements AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: DynamoDBClient;

  constructor(logger: winston.Logger) {
    this.dbClient = new DynamoDBClient({
      region: process.env.AWS_REGION,
      maxAttempts: 5,
    });
    this.logger = logger;
  }

  async getTableNames(): Promise<string[]> {
    const tablesData = await this.dbClient.send(
      new ListTablesCommand({ Limit: 100 })
    );
    if (tablesData.TableNames) {
      return tablesData.TableNames;
    }
    return [];
  }

  async createTable(tableName: string): Promise<any> {
    try {
      const params = {
        AttributeDefinitions: [
          {
            AttributeName: 'sessionId',
            AttributeType: 'S',
          },
          {
            AttributeName: 'timestamp',
            AttributeType: 'S',
          },
        ],
        KeySchema: [
          {
            AttributeName: 'sessionId',
            KeyType: 'HASH',
          },
          {
            AttributeName: 'timestamp',
            KeyType: 'RANGE',
          },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 3,
          WriteCapacityUnits: 3,
        },
        TableName: tableName,
        StreamSpecification: {
          StreamEnabled: false,
        },
      };
      const data = await this.dbClient.send(new CreateTableCommand(params));
      this.logger.info(`Created Table '${tableName}'`);
      return data;
    } catch (err) {
      this.logger.error('Table creation Error!');
      throw new Error(err);
    }
  }

  async putItem(params: IDDBPutItemInput): Promise<any> {
    const eventItem: ITableItem = {};
    Object.keys(params.data).forEach((key) => {
      eventItem[key] = { S: JSON.stringify(params.data[key]) };
    });

    try {
      const data = await this.dbClient.send(
        new PutItemCommand({
          TableName: params.tableName,
          Item: eventItem,
        })
      );
      this.logger.debug(
        `Put JSON with event:${eventItem.event['S']}, in Table:${params.tableName}`
      );
      return data;
    } catch (err) {
      throw this.handleError(err);
    }
  }

  async getItem(params: IDDBGetItemInput): Promise<any> {
    try {
      const rawData = await this.dbClient.send(
        new GetItemCommand({
          TableName: params.tableName,
          Key: { eventId: { S: params.eventId } },
        })
      );
      this.logger.debug('Read Item from Table');
      return rawData;
    } catch (err) {
      throw this.handleError(err);
    }
  }

  async deleteItem(params: IDDBGetItemInput): Promise<any> {
    try {
      const data = await this.dbClient.send(
        new DeleteItemCommand({
          TableName: params.tableName,
          Key: { eventId: { S: params.eventId } },
        })
      );
      this.logger.debug('Deleted Item from Table', data);
      return data;
    } catch (err) {
      throw this.handleError(err);
    }
  }

  async getItemsBySession(params: any): Promise<any> {
    try {
      const queryData: QueryCommandOutput = await this.dbClient.send(
        new QueryCommand({
          TableName: params.tableName,
          KeyConditionExpression: 'sessionId = :sid',
          ExpressionAttributeValues: { ':sid': params.sessionId },
        })
      );
      if (queryData.Items && queryData.Items.length > 0) {
        let items: EventItem[] = [];
        for (let i = 0; i < queryData.Items.length; i++) {
          const e = queryData.Items[i];
          let item: EventItem = {
            event: e.event.S,
            sessionId: e.sessionId.S,
            timestamp: e.timestamp.S,
            duration: e.duration.S ? parseInt(e.duration.S) : -1,
            playhead: e.playhead.S ? parseInt(e.playhead.S) : -1,
          };
          if ('payload' in e) {
            item['payload'] = e.payload.S ? JSON.parse(e.payload.S) : {};
          }
          items[i] = item;
        }
        return items;
      }
      return [];
    } catch (err) {
      throw this.handleError(err);
    }
  }

  handleError(errorObject: any): IHandleErrorOutput {
    this.logger.error(errorObject);
    const errorOutput: IHandleErrorOutput = {
      errorType: ErrorType.ABORT,
      error: errorObject,
    };
    if (errorObject.name) {
      if (
        errorObject.name === 'ResourceNotFoundException' ||
        errorObject.name === 'ResourceInUseException'
      ) {
        errorOutput['errorType'] = ErrorType.CONTINUE;
      }
    }
    return errorOutput;
  }
}
