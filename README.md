# @eyevinn/player-analytics-shared

Shared adapters for the [Eyevinn Player Analytics Specification (EPAS)](https://github.com/Eyevinn/player-analytics-specification) pipeline. Provides pluggable queue and database adapters used by the [Eventsink](https://github.com/Eyevinn/player-analytics-eventsink) and [Worker](https://github.com/Eyevinn/player-analytics-worker).

## Installation

```bash
npm install @eyevinn/player-analytics-shared
```

## Architecture

```
Eventsink (HTTP) ──> Queue Adapter ──> Worker ──> DB Adapter ──> Database
```

This package provides the adapters for both sides:
- **Queue Adapters** — write events from the eventsink, read events in the worker
- **Database Adapters** — write events to persistent storage from the worker

## Queue Adapters

### AWS SQS

```typescript
import { SqsQueueAdapter } from '@eyevinn/player-analytics-shared';
```

| Env Variable | Required | Description |
|---|---|---|
| `AWS_REGION` | Yes | AWS region (e.g., `eu-north-1`) |
| `SQS_QUEUE_URL` | Yes | Full SQS queue URL |
| `SQS_MAX_SOCKETS` | No | Max HTTP sockets for SQS client |
| `SKIP_QUEUE_EXISTS_CHECK` | No | Skip queue validation on startup |

### Redis

```typescript
import { RedisAdapter } from '@eyevinn/player-analytics-shared';
```

Uses [redis-task-queue](https://github.com/nicklasb/redis-task-queue) for job management.

| Env Variable | Required | Description |
|---|---|---|
| `REDIS_HOST` | No | Redis host (default: `localhost`) |
| `REDIS_PORT` | No | Redis port (default: `6379`) |
| `REDIS_PASSWORD` | No | Redis password |

### Beanstalkd

```typescript
import { BeanstalkdAdapter } from '@eyevinn/player-analytics-shared';
```

Uses [node-beanstalk](https://github.com/dvdln/node-beanstalk). Auto-connects on first operation.

## Database Adapters

### ClickHouse

```typescript
import { ClickHouseDBAdapter } from '@eyevinn/player-analytics-shared';
```

Auto-creates tables with MergeTree engine partitioned by month.

| Env Variable | Required | Description |
|---|---|---|
| `CLICKHOUSE_URL` | Yes | Connection URL (e.g., `http://default:password@localhost:8123/epas`) |

**Table schema** (auto-created):
```sql
CREATE TABLE IF NOT EXISTS epas_{shardId} (
  event String,
  sessionId String,
  timestamp DateTime64(3),
  playhead Float64,
  duration Float64,
  live Boolean,
  contentId String,
  userId String,
  deviceId String,
  deviceModel String,
  deviceType String,
  payload String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (sessionId, timestamp)
```

### DynamoDB

```typescript
import { DynamoDBAdapter } from '@eyevinn/player-analytics-shared';
```

| Env Variable | Required | Description |
|---|---|---|
| `AWS_REGION` | Yes | AWS region |
| `AWS_ACCESS_KEY_ID` | Yes | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | Yes | AWS credentials |

### MongoDB

```typescript
import { MongoDBAdapter } from '@eyevinn/player-analytics-shared';
```

| Env Variable | Required | Description |
|---|---|---|
| `MONGODB_URI` | Yes | MongoDB connection URI |

## Adapter Interface

All queue adapters implement `AbstractQueueAdapter`:

```typescript
interface AbstractQueueAdapter {
  pushToQueue(body: Object): Promise<Object>;
  pullFromQueue(): Promise<Object>;
  removeFromQueue(body: Record<string, any>): Promise<boolean>;
  removeFromQueueBatch(messages: Record<string, any>[]): Promise<Object>;
  getEventJSONsFromMessages(body: any[]): Object[];
}
```

All database adapters implement `AbstractDBAdapter`:

```typescript
interface AbstractDBAdapter {
  tableExists(name: string): Promise<boolean>;
  putItem(params: IPutItemInput): Promise<boolean>;
  putItems(params: IPutItemsInput): Promise<boolean>;
  getItem(params: IGetItemInput): Promise<any>;
  deleteItem(params: IGetItemInput): Promise<boolean>;
  getItemsBySession(params: IGetItems): Promise<any[]>;
  handleError(errorObject: any): IHandleErrorOutput;
}
```

## Constants

```typescript
import { TABLE_PREFIX } from '@eyevinn/player-analytics-shared';
// TABLE_PREFIX = 'epas_'
```

Tables are named `epas_{shardId}` where shardId defaults to the player's host domain.

## Development

```bash
npm install
npm test        # Run Jasmine tests
npm run build   # Compile TypeScript
```

## Related Packages

- [@eyevinn/player-analytics-eventsink](https://github.com/Eyevinn/player-analytics-eventsink) — HTTP event ingestion
- [@eyevinn/player-analytics-worker](https://github.com/Eyevinn/player-analytics-worker) — Queue-to-DB processor
- [@eyevinn/player-analytics-client-sdk-web](https://github.com/Eyevinn/player-analytics-client-sdk-web) — Browser SDK
- [@eyevinn/player-analytics-specification](https://github.com/Eyevinn/player-analytics-specification) — EPAS spec

## License

MIT
