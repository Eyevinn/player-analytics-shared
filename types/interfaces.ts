import winston from 'winston';

export interface EventValidator {
  logger: winston.Logger;
  eventSchema: any;
  validateEvent(event: Object): any;
  validateEventList(eventList: Array<Object>): any;
}

export enum ErrorType {
  'ABORT' = 0,
  'CONTINUE' = 1,
}
export interface IHandleErrorOutput {
  errorType: ErrorType;
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
  abstract pullFromQueue(): Promise<Object>;
  abstract removeFromQueue(body: Object): Promise<Object>;
  abstract getEventJSONsFromMessages(body: any[]): Object[];
}
export abstract class AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: any;

  abstract getTableNames(): Promise<string[]>;
  abstract createTable(name: string): Promise<any>;
  abstract putItem(params: Object): Promise<any>;
  abstract getItem(params: Object): Promise<any>;
  abstract deleteItem(params: Object): Promise<any>;
  abstract getItemsBySession(params: Object): Promise<any>;
  abstract handleError(error: any): IHandleErrorOutput;
}
