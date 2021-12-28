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
export interface IPutItemInput {
  tableName: string;
  data: Object;
}
export interface IGetItems {
  sessionId: string;
  tableName: string;
}
export interface IGetItemInput {
  tableName: string;
  sessionId: string;
  timestamp: number;
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
  abstract putItem(params: IPutItemInput): Promise<any>;
  abstract getItem(params: IGetItemInput): Promise<any>;
  abstract deleteItem(params: IGetItemInput): Promise<any>;
  abstract getItemsBySession(params: IGetItems): Promise<any>;
  abstract handleError(error: any): IHandleErrorOutput;
}
