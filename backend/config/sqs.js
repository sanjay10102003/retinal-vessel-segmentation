require("dotenv").config();

const { SQSClient } = require("@aws-sdk/client-sqs");

const sqs = new SQSClient({
    region: process.env.AWS_REGION,
});

module.exports = { sqs };