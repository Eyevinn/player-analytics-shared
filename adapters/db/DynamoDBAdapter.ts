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
  DescribeTableCommand,
  DescribeTableCommandInput
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import winston from 'winston';
import {
  AbstractDBAdapter,
  IGetItemInput,
  IGetItems,
  IPutItemInput,
  IPutItemsInput,
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

  async tableExists(name: string): Promise<boolean> {
    try {
      const params: DescribeTableCommandInput = { TableName: name };
      const tablesData = await this.dbClient.send(new DescribeTableCommand(params));
      if (tablesData['Table'] && tablesData['Table'].TableStatus === 'ACTIVE') return true;
      return false;
    } catch (awsError) {
      if (awsError.name && awsError.name === 'ResourceNotFoundException') {
        return false;
      } else {
        this.logger.error(awsError);
        throw new Error(awsError);
      }
    }
  }

  async putItem(params: IPutItemInput): Promise<boolean> {
    try {
      const data = await this.dbClient.send(
        new PutItemCommand({
          TableName: params.tableName,
          Item: marshall(params.data),
        })
      );
      return data.$metadata.httpStatusCode === 200;
    } catch (err) {
      throw this.handleError(err);
    }
  }

  async putItems(params: IPutItemsInput): Promise<boolean> {
    try {
      if (!params.data || params.data.length === 0) {
        this.logger.warn('No items provided for batch insert');
        return true;
      }

      this.logger.debug(`Batch inserting ${params.data.length} items into ${params.tableName}`);
      
      // DynamoDB batch write has a limit of 25 items per request
      const batchSize = 25;
      const batches: Object[][] = [];
      
      for (let i = 0; i < params.data.length; i += batchSize) {
        batches.push(params.data.slice(i, i + batchSize));
      }
      
      // Process all batches
      for (const batch of batches) {
        const promises = batch.map(item => 
          this.dbClient.send(
            new PutItemCommand({
              TableName: params.tableName,
              Item: marshall(item),
            })
          )
        );
        
        await Promise.all(promises);
      }
      
      this.logger.debug(`Successfully batch inserted ${params.data.length} items into ${params.tableName}`);
      return true;
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

  async deleteItem(params: IGetItemInput): Promise<boolean> {
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
      return data.$metadata.httpStatusCode === 200;
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
