import { Logger } from "winston";
import { AbstractDBAdapter, IHandleErrorOutput, ErrorType, IPutItemInput, IGetItemInput, IGetItems } from "../../types/interfaces";
import { MongoClient } from "mongodb";

const DB_NAME = "EPAS";

export class MongoDBAdapter implements AbstractDBAdapter {
  logger: Logger;
  dbClient: MongoClient;

  constructor(logger: Logger) {
    const connectionString = process.env.MONGODB_URI || "mongodb://localhost";
    this.dbClient = new MongoClient(connectionString);
    this.connect()
    this.logger = logger;
  }

  private async isConnected(): Promise<boolean> {
    return !!await this.dbClient.db("admin").command({ ping: 1 });
  }

  private async connect(): Promise<void> {
    await this.dbClient.connect();
  }

  public async getTableNames(): Promise<string[]> {
    if (!await this.isConnected()) await this.connect();
    const collections = await this.dbClient.db().collections();
    return collections.map((collection) => collection.collectionName);
  }

  public async createTable(tableName: string): Promise<any> {
    try {
      const collections = await this.getTableNames();
      if (collections.includes(tableName)) return tableName;
      const res = await this.dbClient.db().createCollection(tableName);
      return res.collectionName;
    } catch (error) {
      this.handleError(error);
    }
  }

  public async putItem({ tableName, data }: IPutItemInput): Promise<any> {
    try {
      const collection = await this.dbClient.db().collection(tableName);
      const result = await collection.insertOne(data);
      return result;
    } catch (error) {
      this.handleError(error);
    }
  }

  public async getItem({ sessionId, tableName, timestamp }: IGetItemInput): Promise<any> {
    try {
      const collection = await this.dbClient.db().collection(tableName);
      const result = await collection.findOne({ sessionId: sessionId, timestamp: timestamp });
      return result;
    } catch (error) {
      this.handleError(error);
    }
  }

  public async deleteItem({ sessionId, tableName, timestamp }: IGetItemInput): Promise<any> {
    try {
      const collection = await this.dbClient.db().collection(tableName);
      const result = await collection.deleteOne({ sessionId: sessionId, timestamp: timestamp });
      return result.acknowledged;
    } catch (error) {
      this.handleError(error);
    }
  }
  public async getItemsBySession({ tableName, sessionId }: IGetItems): Promise<any> {
    try {
      const collection = await this.dbClient.db().collection(tableName);
      return await collection.find({ sessionId: sessionId }).toArray();
    } catch (error) {
      this.handleError(error);
    }
  }

  public handleError(error: any): IHandleErrorOutput {
    this.logger.error(error);
    return {
      errorType: ErrorType.ABORT,
      error: error,
    };
  }

}
