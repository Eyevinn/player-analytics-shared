import {
  CreateTableCommand,
  ListTablesCommand,
  PutItemCommand,
  DynamoDBClient,
  GetItemCommand,
  DeleteItemCommand,
  CreateTableCommandOutput,
  PutItemCommandOutput,
  GetItemCommandOutput,
  DeleteItemCommandOutput,
  QueryCommand,
  QueryCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { AwsError, mockClient } from 'aws-sdk-client-mock';
import { DynamoDBAdapter } from '../../adapters/db/DynamoDBAdapter';
import { ErrorType } from '../../types/interfaces';
import Logger from '../../util/logger';

const ddbMock = mockClient(DynamoDBClient);

describe('Dynamo DB Adapter', () => {
  beforeEach(() => {
    process.env.AWS_REGION = 'us-east-1';
    ddbMock.reset();
  });

  it('should return list of table names in database', async () => {
    const mockTables = ['table_1', 'table_2', 'table_3'];
    const DDBReply = {
      MessageId: '12345678-4444-5555-6666-111122223333',
      TableNames: mockTables,
    };
    const adapter = new DynamoDBAdapter(Logger);
    ddbMock.on(ListTablesCommand).resolves(DDBReply);
    const result = await adapter.getTableNames();
    expect(result).toEqual(mockTables);
  });

  it('should create a table in database', async () => {
    const tableName = 'table_1';
    const DDBReply: CreateTableCommandOutput = {
      $metadata: {},
      TableDescription: {
        TableName: tableName,
      },
    };

    const adapter = new DynamoDBAdapter(Logger);
    ddbMock.on(CreateTableCommand).resolves(DDBReply);
    const result = await adapter.createTable(tableName);
    expect(result).toEqual(DDBReply.TableDescription?.TableName || tableName);
  });

  it('should put item to database', async () => {
    const DDBReply: PutItemCommandOutput = {
      $metadata: {},
    };
    const adapter = new DynamoDBAdapter(Logger);
    const mockEvent = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
      host: 'mock.tenant.mock',
    };
    ddbMock.on(PutItemCommand).resolves(DDBReply);
    const result = await adapter.putItem({
      tableName: 'table_1',
      data: mockEvent,
    });
    expect(result).toBeTrue();
  });

  it('should label errorType with "continue" when allowed error occurs', async () => {
    const DDBReply: AwsError = {
      Type: 'Sender',
      Code: 'ResourceNotFoundException',
      name: 'ResourceNotFoundException',
      $fault: 'client',
      $metadata: {
        httpStatusCode: 400,
        requestId: 'df840ab9-e68b-5c0e-b4a0-5094f2dfaee8',
        attempts: 1,
        totalRetryDelay: 0,
      },
    };
    const adapter = new DynamoDBAdapter(Logger);
    const mockEvent = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
      host: 'mock.tenant.mock',
    };
    ddbMock.on(PutItemCommand).rejects(DDBReply);
    try {
      const result = await adapter.putItem({
        tableName: 'table_1',
        data: mockEvent,
      });
    } catch (err) {
      expect(err.errorType).toEqual(ErrorType.CONTINUE);
      expect(err.error.Code).toEqual('ResourceNotFoundException');
    }
  });

  it('should label errorType with "abort" when non-allowed error occurs', async () => {
    const DDBReply: AwsError = {
      Type: 'Sender',
      Code: 'RequestLimitExceeded',
      name: 'RequestLimitExceeded',
      $fault: 'client',
      $metadata: {
        httpStatusCode: 400,
        requestId: 'df840ab9-e68b-5c0e-b4a0-5094f2dfaee8',
        attempts: 1,
        totalRetryDelay: 0,
      },
    };
    const adapter = new DynamoDBAdapter(Logger);
    const mockEvent = {
      event: 'loading',
      timestamp: 0,
      playhead: 0,
      duration: 0,
      host: 'mock.tenant.mock',
    };
    ddbMock.on(PutItemCommand).rejects(DDBReply);
    try {
      const result = await adapter.putItem({
        tableName: 'table_1',
        data: mockEvent,
      });
    } catch (err) {
      expect(err.errorType).toEqual(ErrorType.ABORT);
      expect(err.error.Code).toEqual('RequestLimitExceeded');
    }
  });

  it('should get item from database', async () => {
    const DDBReply: GetItemCommandOutput = {
      $metadata: {},
      Item: { eventId: { S: '123-123-123-123' } },
    };
    const adapter = new DynamoDBAdapter(Logger);
    const mockId = '123-123-123-123';
    ddbMock.on(GetItemCommand).resolves(DDBReply);
    const result = await adapter.getItem({
      tableName: 'table_1',
      sessionId: mockId,
      timestamp: 0,
    });
    expect(result).toEqual({
      $metadata: {},
      Item: { eventId: { S: '123-123-123-123' } },
    });
  });

  it('should delete item in database', async () => {
    const DDBReply: DeleteItemCommandOutput = {
      $metadata: {
        httpStatusCode: 200,
      },
    };
    const adapter = new DynamoDBAdapter(Logger);
    const mockId = '123-123-123-123';
    ddbMock.on(DeleteItemCommand).resolves(DDBReply);
    const result = await adapter.deleteItem({
      tableName: 'table_1',
      sessionId: mockId,
      timestamp: 0,
    });
    expect(result).toBeTrue();
  });

  it('should get items from db with a specific sessionId and convert them to valid event objects', async () => {
    const DDBReply: QueryCommandOutput = {
      $metadata: {},
      Items: [
        {
          event: { S: 'playing' },
          sessionId: { S: '123-214-234' },
          timestamp: { N: '1640191099' },
          playhead: { N: '1' },
          duration: { N: '0' },
        },
        {
          event: { S: 'playing' },
          sessionId: { S: '123-214-234' },
          timestamp: { N: '1640193099' },
          playhead: { N: '3' },
          duration: { N: '0' },
        },
        {
          event: { S: 'paused' },
          sessionId: { S: '123-214-234' },
          timestamp: { N: '1640192099' },
          playhead: { N: '2' },
          duration: { N: '0' },
        },
      ],
    };
    const adapter = new DynamoDBAdapter(Logger);
    ddbMock.on(QueryCommand).resolves(DDBReply);
    const result = await adapter.getItemsBySession({
      tableName: 'table_1',
      sessionId: '123-214-234',
    });
    expect(result).toEqual([
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
