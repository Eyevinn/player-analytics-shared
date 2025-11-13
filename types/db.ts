import winston from 'winston';

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

export interface IPutItemsInput {
  tableName: string;
  data: Object[];
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

export abstract class AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: any;

  abstract tableExists(name: string): Promise<boolean>;
  abstract putItem(params: IPutItemInput): Promise<boolean>;
  abstract putItems(params: IPutItemsInput): Promise<boolean>;
  abstract getItem(params: IGetItemInput): Promise<any>;
  abstract deleteItem(params: IGetItemInput): Promise<boolean>;
  abstract getItemsBySession(params: IGetItems): Promise<any[]>;
  abstract handleError(error: any): IHandleErrorOutput;
}
