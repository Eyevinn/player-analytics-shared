import winston from 'winston';

export interface EventValidator {
  logger: winston.Logger;
  eventSchema: any;
  validateEvent(event: Object): any;
  validateEventList(eventList: Array<Object>): any;
}

export interface IHandleErrorOutput {
  errorType: string;
  error: Object;
}

export interface IDDBPutItemInput {
  tableName: string;
  data: Object;
}
export interface IDDBGetItemInput {
  tableName: string;
  eventId: string;
}

export abstract class AbstractQueueAdapter {
  logger: winston.Logger;
  client: any;
  abstract pushToQueue(body: Object): Promise<Object>;
  abstract pullFromQueue(body: Object): Promise<Object>;
  abstract removeFromQueue(body: Object): Promise<Object>;
}
export abstract class AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: any;

  abstract getTableNames(): Promise<string[]>;
  abstract createTable(name: string): Promise<void>;
  abstract putItem(params: Object): Promise<void>;
  abstract getItem(params: Object): Promise<any>;
  abstract deleteItem(params: Object): Promise<void>;
  abstract getItemsBySession(params: Object): Promise<any>;
  abstract handleError(error: any): IHandleErrorOutput;
}
