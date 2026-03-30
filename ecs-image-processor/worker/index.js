const axios = require("axios");
const FormData = require("form-data");
const {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} = require("@aws-sdk/client-sqs");
const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} = require("@aws-sdk/client-s3");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  GetCommand,
  UpdateCommand,
} = require("@aws-sdk/lib-dynamodb");

const REGION = process.env.AWS_REGION || "us-east-1";
const SQS_QUEUE_URL =
  process.env.SQS_QUEUE_URL ||
  "https://sqs.us-east-1.amazonaws.com/485039560092/data-608-queue";
const DYNAMO_TABLE_NAME = process.env.DYNAMO_TABLE_NAME || "jobs";
const OUTPUT_BUCKET = process.env.OUTPUT_BUCKET || "data-608-project-bucket";
const LOCAL_API_URL =
  process.env.LOCAL_API_URL || "http://127.0.0.1:8000/predict";

if (!SQS_QUEUE_URL) throw new Error("Missing SQS_QUEUE_URL");
if (!DYNAMO_TABLE_NAME) throw new Error("Missing DYNAMO_TABLE_NAME");
if (!OUTPUT_BUCKET) throw new Error("Missing OUTPUT_BUCKET");

const sqs = new SQSClient({ region: REGION });
const s3 = new S3Client({ region: REGION });
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({ region: REGION }));

function log(level, message, meta = {}) {
  const timestamp = new Date().toISOString();
  console.log(
    JSON.stringify({
      timestamp,
      level,
      message,
      ...meta,
    }),
  );
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseS3Uri(s3Uri) {
  if (!s3Uri.startsWith("s3://")) {
    throw new Error(`Invalid S3 URI: ${s3Uri}`);
  }

  const path = s3Uri.replace("s3://", "");
  const slashIndex = path.indexOf("/");
  if (slashIndex === -1) {
    throw new Error(`Invalid S3 URI: ${s3Uri}`);
  }

  return {
    bucket: path.slice(0, slashIndex),
    key: path.slice(slashIndex + 1),
  };
}

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function getJob(jobId) {
  log("INFO", "Fetching job from DynamoDB", { jobId });

  const result = await ddb.send(
    new GetCommand({
      TableName: DYNAMO_TABLE_NAME,
      Key: { job_id: jobId },
    }),
  );

  if (!result.Item) {
    throw new Error(`Job not found: ${jobId}`);
  }

  log("INFO", "Job fetched successfully", {
    jobId,
    status: result.Item.status,
    input_image_path: result.Item.input_image_path,
  });

  return result.Item;
}

async function updateJob(jobId, fields) {
  const keys = Object.keys(fields);
  if (!keys.length) return;

  log("INFO", "Updating job in DynamoDB", { jobId, fields });

  const updateExpressions = [];
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};

  for (const key of keys) {
    updateExpressions.push(`#${key} = :${key}`);
    expressionAttributeNames[`#${key}`] = key;
    expressionAttributeValues[`:${key}`] = fields[key];
  }

  await ddb.send(
    new UpdateCommand({
      TableName: DYNAMO_TABLE_NAME,
      Key: { job_id: jobId },
      UpdateExpression: `SET ${updateExpressions.join(", ")}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
    }),
  );

  log("INFO", "Job updated successfully", { jobId });
}

async function downloadFromS3(s3Uri, jobId) {
  const { bucket, key } = parseS3Uri(s3Uri);

  log("INFO", "Downloading input image from S3", {
    jobId,
    bucket,
    key,
  });

  const result = await s3.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    }),
  );

  const buffer = await streamToBuffer(result.Body);

  log("INFO", "Downloaded input image from S3", {
    jobId,
    bucket,
    key,
    sizeBytes: buffer.length,
    contentType: result.ContentType || "application/octet-stream",
  });

  return {
    buffer,
    contentType: result.ContentType || "application/octet-stream",
    fileName: key.split("/").pop() || "input.jpg",
  };
}

async function sendImageToApi(buffer, contentType, fileName, jobId) {
  log("INFO", "Sending image to local API", {
    jobId,
    fileName,
    contentType,
    sizeBytes: buffer.length,
    apiUrl: LOCAL_API_URL,
  });

  const form = new FormData();

  form.append("file", buffer, {
    filename: fileName,
    contentType,
  });

  const response = await axios.post(LOCAL_API_URL, form, {
    headers: form.getHeaders(),
    responseType: "arraybuffer",
    maxBodyLength: Infinity,
    maxContentLength: Infinity,
  });

  const responseBuffer = Buffer.from(response.data);

  log("INFO", "Received processed image from local API", {
    jobId,
    statusCode: response.status,
    outputContentType: response.headers["content-type"] || contentType,
    outputSizeBytes: responseBuffer.length,
  });

  return {
    buffer: responseBuffer,
    contentType: response.headers["content-type"] || contentType,
  };
}

async function uploadOutput(jobId, buffer, contentType) {
  const key = `processed/${jobId}`;

  log("INFO", "Uploading processed image to S3", {
    jobId,
    bucket: OUTPUT_BUCKET,
    key,
    sizeBytes: buffer.length,
    contentType,
  });

  await s3.send(
    new PutObjectCommand({
      Bucket: OUTPUT_BUCKET,
      Key: key,
      Body: buffer,
      ContentType: contentType,
    }),
  );

  const outputPath = `s3://${OUTPUT_BUCKET}/${key}`;

  log("INFO", "Processed image uploaded successfully", {
    jobId,
    outputPath,
  });

  return outputPath;
}

async function processJob(jobId) {
  const startedAt = Date.now();

  log("INFO", "Starting job processing", { jobId });

  await updateJob(jobId, {
    status: "PROCESSING",
    updated_at: new Date().toISOString(),
  });

  const job = await getJob(jobId);
  console.log("****************", job);
  log("INFO", "**************", { job });

  if (!job.input_image_path) {
    throw new Error(`imagePath missing for jobId=${jobId}`);
  }

  log("INFO", "Job input validated", {
    jobId,
    input_image_path: job.input_image_path,
  });

  const inputImage = await downloadFromS3(job.input_image_path, jobId);

  const processedImage = await sendImageToApi(
    inputImage.buffer,
    inputImage.contentType,
    inputImage.fileName,
    jobId,
  );

  const outputPath = await uploadOutput(
    jobId,
    processedImage.buffer,
    processedImage.contentType,
  );

  await updateJob(jobId, {
    status: "COMPLETED",
    output_path: outputPath,
    updated_at: new Date().toISOString(),
  });

  log("INFO", "Job completed successfully", {
    jobId,
    outputPath,
    durationMs: Date.now() - startedAt,
  });
}

async function pollQueue() {
  log("INFO", "Worker started", {
    region: REGION,
    queueUrl: SQS_QUEUE_URL,
    dynamoTable: DYNAMO_TABLE_NAME,
    outputBucket: OUTPUT_BUCKET,
    apiUrl: LOCAL_API_URL,
  });

  while (true) {
    try {
      log("INFO", "Polling SQS for messages");

      const response = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: SQS_QUEUE_URL,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 20,
          VisibilityTimeout: 120,
        }),
      );

      const messages = response.Messages || [];

      if (!messages.length) {
        log("INFO", "No messages received from queue");
        continue;
      }

      log("INFO", "Messages received from queue", {
        count: messages.length,
      });

      for (const message of messages) {
        let jobId = null;

        try {
          log("INFO", "Processing SQS message", {
            messageId: message.MessageId,
          });

          const body = JSON.parse(message.Body || "{}");
          jobId = body?.data?.job_id;

          log("INFO", "Parsed SQS message body", {
            messageId: message.MessageId,
            jobId,
            body,
          });

          if (!jobId) {
            throw new Error("Message missing jobId");
          }

          await processJob(jobId);

          await sqs.send(
            new DeleteMessageCommand({
              QueueUrl: SQS_QUEUE_URL,
              ReceiptHandle: message.ReceiptHandle,
            }),
          );

          log("INFO", "SQS message deleted after successful processing", {
            messageId: message.MessageId,
            jobId,
          });
        } catch (err) {
          log("ERROR", "Message processing failed", {
            messageId: message.MessageId,
            jobId,
            error: err.message,
            stack: err.stack,
          });

          try {
            if (jobId) {
              await updateJob(jobId, {
                status: "FAILED",
                error_message: err.message,
                updated_at: new Date().toISOString(),
              });

              log("INFO", "Marked job as FAILED in DynamoDB", { jobId });
            }
          } catch (innerErr) {
            log("ERROR", "Failed to update job status in DynamoDB", {
              jobId,
              error: innerErr.message,
              stack: innerErr.stack,
            });
          }
        }
      }
    } catch (err) {
      log("ERROR", "Polling error", {
        error: err.message,
        stack: err.stack,
      });
      await sleep(5000);
    }
  }
}

pollQueue().catch((err) => {
  log("ERROR", "Fatal worker error", {
    error: err.message,
    stack: err.stack,
  });
  process.exit(1);
});
