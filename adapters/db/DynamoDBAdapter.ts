import {
  CreateTableCommand,
  ListTablesCommand,
  PutItemCommand,
  DynamoDBClient,
  GetItemCommand,
  DeleteItemCommand,
  QueryCommand,
  QueryCommandInput,
  QueryCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import winston from 'winston';
import {
  AbstractDBAdapter,
  IGetItemInput,
  IGetItems,
  IPutItemInput,
  IHandleErrorOutput,
  ErrorType,
} from '../../types/interfaces';

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
            AttributeType: 'N',
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

  async putItem(params: IPutItemInput): Promise<any> {
    try {
      const data = await this.dbClient.send(
        new PutItemCommand({
          TableName: params.tableName,
          Item: marshall(params.data),
        })
      );
      return data;
    } catch (err) {
      throw this.handleError(err);
    }
  }

  async getItem(params: IGetItemInput): Promise<any> {
    try {
      const data = await this.dbClient.send(
        new GetItemCommand({
          TableName: params.tableName,
          Key: marshall({
            sessionId: params.sessionId,
            timestamp: params.timestamp,
          }),
        })
      );
      this.logger.debug('Read Item from Table');
      return data;
    } catch (err) {
      throw this.handleError(err);
    }
  }

  async deleteItem(params: IGetItemInput): Promise<any> {
    try {
      const data = await this.dbClient.send(
        new DeleteItemCommand({
          TableName: params.tableName,
          Key: marshall({
            sessionId: params.sessionId,
            timestamp: params.timestamp,
          }),
        })
      );
      this.logger.debug('Deleted Item from Table', data);
      return data;
    } catch (err) {
      throw this.handleError(err);
    }
  }

  async getItemsBySession(params: IGetItems): Promise<any[]> {
    try {
      const inputData: QueryCommandInput = {
        TableName: params.tableName,
        KeyConditionExpression: '#sid = :sid',
        ExpressionAttributeNames: {
          '#sid': 'sessionId',
        },
        ExpressionAttributeValues: marshall({
          ':sid': params.sessionId,
        }),
      };
      const queryData: QueryCommandOutput = await this.dbClient.send(
        new QueryCommand(inputData)
      );
      if (queryData.Items && queryData.Items.length > 0) {
        let items: any[] = [];
        for (let i = 0; i < queryData.Items.length; i++) {
          items[i] = unmarshall(queryData.Items[i]);
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
