const { DynamoDBClient, CreateTableCommand, DescribeTableCommand } = require("@aws-sdk/client-dynamodb");

const client = new DynamoDBClient({
  region: "us-east-1",
  endpoint: "http://localhost:8001",
  credentials: {
    accessKeyId: "dummy",
    secretAccessKey: "dummy",
  },
});

async function migrate() {
  const tableName = "jobs";

  try {
    await client.send(new DescribeTableCommand({ TableName: tableName }));
    console.log(`Table "${tableName}" already exists`);
    return;
  } catch (err) {
    if (err.name !== "ResourceNotFoundException") {
      throw err;
    }
  }

  await client.send(
    new CreateTableCommand({
      TableName: tableName,
      AttributeDefinitions: [
        { AttributeName: "job_id", AttributeType: "S" },
      ],
      KeySchema: [
        { AttributeName: "job_id", KeyType: "HASH" },
      ],
      BillingMode: "PAY_PER_REQUEST",
    })
  );

  console.log(`Table "${tableName}" created successfully`);
}

migrate().catch((err) => {
  console.error("Migration failed:", err);
});