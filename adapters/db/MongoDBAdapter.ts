import { Logger } from "winston";
import { AbstractDBAdapter, IHandleErrorOutput, ErrorType, IPutItemInput, IGetItemInput, IGetItems } from "../../types/interfaces";
import monk, { IMonkManager } from "monk";

export class MongoDBAdapter implements AbstractDBAdapter {
  logger: Logger;
  dbClient: IMonkManager;

  constructor(logger: Logger) {
    const connectionString = process.env.MONGODB_URI || "localhost";
    this.dbClient = monk(connectionString);
    this.logger = logger;
  }

  public async getTableNames(): Promise<string[]> {
    const collections = await this.dbClient.listCollections();
    return collections.map((collection) => collection.name);
  }

  public async createTable(tableName: string): Promise<any> {
    try {
      const collections = await this.getTableNames();
      if (collections.includes(tableName)) return true;
      return await this.dbClient.create(tableName);
    } catch (error) {
      this.handleError(error);
    }
  }

  public async putItem({ tableName, data }: IPutItemInput): Promise<any> {
    try {
      const collection = await this.dbClient.get(tableName);
      return await collection.insert(data);
    } catch (error) {
      this.handleError(error);
    }
  }

  public async getItem({ sessionId, tableName, timestamp }: IGetItemInput): Promise<any> {
    try {
      const collection = await this.dbClient.get(tableName);
      return await collection.findOne({ sessionId: sessionId, timestamp: timestamp });
    } catch (error) {
      this.handleError(error);
    }
  }

  public async deleteItem({ sessionId, tableName, timestamp }: IGetItemInput): Promise<any> {
    try {
      const collection = await this.dbClient.get(tableName);
      return await collection.remove({ sessionId: sessionId, timestamp: timestamp });
    } catch (error) {
      this.handleError(error);
    }
  }
  public async getItemsBySession({ tableName, sessionId }: IGetItems): Promise<any> {
    try {
      const collection = await this.dbClient.get(tableName);
      return await collection.find({ sessionId: sessionId });
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
