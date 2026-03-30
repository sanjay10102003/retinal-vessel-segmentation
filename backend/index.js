require("dotenv").config();

const express = require("express");
const app = express();
const multer = require("multer");
const crypto = require("crypto");
const path = require("path");
const cors = require("cors");
const { sqs } = require(".//sqs");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const { PutCommand, GetCommand } = require("@aws-sdk/lib-dynamodb");
const { dynamodb } = require("./config/dynamodb");
const { s3 } = require("./config/s3");
const { SendMessageCommand } = require("@aws-sdk/client-sqs");
const { PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");

const PORT = process.env.PORT;
app.use(cors());
app.use(express.json());

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024, // 10 MB
  },
  fileFilter: (req, file, cb) => {
    if (!file.mimetype.startsWith("image/")) {
      return cb(new Error("Only image files are allowed"));
    }
    cb(null, true);
  },
});

app.post("/api/image", upload.single("image"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({
        message: "No image uploaded",
      });
    }

    const job_id = crypto.randomUUID();
    const ext = path.extname(req.file.originalname) || ".jpg";
    const s3Key = `uploads/${job_id}${ext}`;

    const uploadParams = {
      Bucket: process.env.S3_BUCKET_NAME,
      Key: s3Key,
      Body: req.file.buffer,
      ContentType: req.file.mimetype,
    };

    await s3.send(new PutObjectCommand(uploadParams));

    const input_image_path = `s3://${process.env.S3_BUCKET_NAME}/${s3Key}`;
    const output_path = "";
    const status = "QUEUED";
    const timestamp = new Date().toISOString();

    await dynamodb.send(
      new PutCommand({
        TableName: "jobs",
        Item: {
          job_id,
          input_image_path,
          output_path,
          status,
          created_at: timestamp,
          updated_at: timestamp,
        },
      }),
    );

    await sqs.send(
      new SendMessageCommand({
        QueueUrl: process.env.SQS_QUEUE_URL,
        MessageBody: JSON.stringify({
          eventType: "IMAGE_PROCESS",
          data: { job_id, image_path: input_image_path },
        }),
      }),
    );
    console.log(input_image_path);

    return res.status(200).json({
      message: "Image uploaded successfully",
      job_id,
      status,
    });
  } catch (error) {
    console.error("Upload error:", error);

    return res.status(500).json({
      message: "Failed to upload image",
      error: error.message,
    });
  }
});

/**
 * Ping / polling endpoint
 * Frontend will call every 2 seconds
 */
app.get("/api/image/status/:job_id", async (req, res) => {
  try {
    const { job_id } = req.params;

    const result = await dynamodb.send(
      new GetCommand({
        TableName: "jobs",
        Key: { job_id },
      }),
    );

    if (!result.Item) {
      return res.status(404).json({
        message: "Job not found",
      });
    }

    const job = result.Item;
    const status = job.status;

    let image_url = null;
    if (status === "COMPLETED" && job.output_path) {
      if (job.output_path.startsWith("s3://")) {
        const s3Path = job.output_path.replace("s3://", "");
        const firstSlash = s3Path.indexOf("/");
        const bucket = s3Path.substring(0, firstSlash);
        const key = s3Path.substring(firstSlash + 1);

        image_url = await getSignedUrl(
          s3,
          new GetObjectCommand({
            Bucket: bucket,
            Key: key,
          }),
          { expiresIn: 3600 },
        );
      } else {
        image_url = job.output_path;
      }
    }

    return res.status(200).json({
      job_id: job.job_id,
      status,
      image_url,
      message:
        status === "QUEUED"
          ? "Job is queued"
          : status === "PROCESSING"
            ? "Image is processing"
            : status === "COMPLETED"
              ? "Image processing completed"
              : status === "FAILED"
                ? "Image processing failed"
                : "Unknown status",
    });
  } catch (error) {
    console.error("Status check error:", error);

    return res.status(500).json({
      message: "Failed to fetch job status",
      error: error.message,
    });
  }
});

app.get("/api/test", (req, res) => {
  res.json({ message: "API is working!" });
});

// Multer / file size / type errors
app.use((err, req, res, next) => {
  if (err instanceof multer.MulterError) {
    if (err.code === "LIMIT_FILE_SIZE") {
      return res.status(400).json({
        message: "File size must be less than or equal to 10 MB",
      });
    }
  }

  if (err.message === "Only image files are allowed") {
    return res.status(400).json({
      message: err.message,
    });
  }

  return res.status(500).json({
    message: "Something went wrong",
    error: err.message,
  });
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
