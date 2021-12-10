import { SQSClient } from "@aws-sdk/client-sqs";
import { SqsQueueAdapter } from "../..";
import Logger from "../../util/logger";

describe("SQS Queue Adapter", () => {
  beforeEach(() => {
    process.env.AWS_REGION = "us-east-1";
    process.env.SQS_QUEUE_URL =
      "https://sqs.us-east-1.amazonaws.com/1234/test-queue";
  });

  it("should not push to queue if sqs queue env is not set", async () => {
    process.env.SQS_QUEUE_URL = undefined;
    spyOn(SQSClient.prototype, "send").and.callFake(function () {
      return Promise.resolve({
        message: "SQS_QUEUE_URL is undefined",
      });
    });
    const queueAdapter = new SqsQueueAdapter(Logger);
    const mockEvent = {
      event: "loading",
      timestamp: 0,
      playhead: 0,
      duration: 0,
    };
    let result = await queueAdapter.pushToQueue(mockEvent);
    expect(result).toEqual({ message: "SQS_QUEUE_URL is undefined" });
  });

  it("should not push to queue if AWS region env is not set", async () => {
    process.env.AWS_REGION = undefined;
    spyOn(SQSClient.prototype, "send").and.callFake(function () {
      return Promise.resolve({
        message: "AWS_REGION is undefined",
      });
    });
    const queueAdapter = new SqsQueueAdapter(Logger);
    const mockEvent = {
      event: "loading",
      timestamp: 0,
      playhead: 0,
      duration: 0,
    };
    let result = await queueAdapter.pushToQueue(mockEvent);
    expect(result).toEqual({ message: "AWS_REGION is undefined" });
  });

  it("should catch error when sending message fails", async () => {
    spyOn(SQSClient.prototype, "send").and.callFake(function () {
      return Promise.reject({
        message: "SQS MOCK ERROR MESSAGE!",
      });
    });
    const queueAdapter = new SqsQueueAdapter(Logger);
    const mockEvent = {
      event: "loading",
      timestamp: 0,
      playhead: 0,
      duration: 0,
    };
    let result = await queueAdapter.pushToQueue(mockEvent);
    expect(result).toEqual({ message: "SQS MOCK ERROR MESSAGE!" });
  });
});
