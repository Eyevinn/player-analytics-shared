import {
  CreateTableCommand,
  ListTablesCommand,
  PutItemCommand,
  DynamoDBClient,
  GetItemCommand,
  DeleteItemCommand,
  AttributeValue,
  CreateTableCommandOutput,
  PutItemCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { AwsError, mockClient } from 'aws-sdk-client-mock';
import { DynamoDBAdapter } from '../../adapters/DynamoDBAdapter';
import Logger from '../../util/logger';

const sqsMock = mockClient(DynamoDBClient);

describe('Dynamo DB Adapter', () => {
  beforeEach(() => {
    process.env.AWS_REGION = 'us-east-1';
    process.env.DB_TYPE = 'SQS';
    sqsMock.reset();
  });

  afterEach(() => {
    delete process.env.AWS_REGION;
    delete process.env.DB_TYPE;
  });

  it('should return list of table names in database', async () => {
    const mockTables = ['table_1', 'table_2', 'table_3'];
    const DDBReply = {
      MessageId: '12345678-4444-5555-6666-111122223333',
      TableNames: mockTables,
    };
    const adapter = new DynamoDBAdapter(Logger);
    sqsMock.on(ListTablesCommand).resolves(DDBReply);
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
    sqsMock.on(CreateTableCommand).resolves(DDBReply);
    const result = await adapter.createTable(tableName);
    expect(result).toEqual(DDBReply);
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
    sqsMock.on(PutItemCommand).resolves(DDBReply);
    const result = await adapter.putItem({
      tableName: 'table_1',
      data: mockEvent,
    });
    expect(result).toEqual({ $metadata: {} });
  });
});
