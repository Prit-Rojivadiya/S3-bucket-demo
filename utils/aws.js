const AWS = require("aws-sdk");
const axios = require("axios");
const { Readable } = require("stream");
require("dotenv").config();
const {
  S3Client,
  CreateMultipartUploadCommand,
  PutObjectCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
} = require("@aws-sdk/client-s3");
const archiver = require("archiver");
// const { IncomingForm } = require("formidable");

const {
  AWS_DEFAULT_REGION,
  S3_BUCKET_PRIVATE,
  AWS_APP_USER_ACCESS_KEY,
  AWS_APP_USER_SECRET_ACCESS_KEY,
} = process.env;

AWS.config.region = AWS_DEFAULT_REGION;

AWS.config.update({
  accessKeyId: AWS_APP_USER_ACCESS_KEY,
  secretAccessKey: AWS_APP_USER_SECRET_ACCESS_KEY,
});

const ONE_DAY_IN_SEC = 60 * 60 * 24;

const fileUrlFromPath = ({ filePath = "", bucket = S3_BUCKET_PRIVATE }) =>
  `https://${bucket}.s3.amazonaws.com/${filePath}`;

const s3Uploader = (filePath, fileType) => {
  const S3 = new AWS.S3({ signatureVersion: "v4" });
  return new Promise((resolve, reject) => {
    S3.getSignedUrl(
      "getObject",
      {
        Bucket: S3_BUCKET_PRIVATE,
        Key: filePath,
        Expires: 300,
        // ContentType: fileType,
      },
      (err, requestUrl) => {
        if (err) {
          reject(err);
        }
        if (requestUrl) {
          resolve({ requestUrl, fileUrl: fileUrlFromPath({ filePath }) });
        }
      }
    );
  });
};

const s3UploadBlob = async ({ filePath, fileType, data, size }) => {
  const keyName = filePath;
  const client = new S3Client({
    credentials: {
      accessKeyId: AWS_APP_USER_ACCESS_KEY,
      secretAccessKey: AWS_APP_USER_SECRET_ACCESS_KEY,
    },
    region: AWS_DEFAULT_REGION,
  });

  const params = {
    Bucket: S3_BUCKET_PRIVATE,
    Key: keyName,
    Body: data,
  };
  const command = new PutObjectCommand(params);
  try {
    const data = await client.send(command);
    // process data.

    return data;
  } catch (error) {
    // error handling.
  } finally {
    // finally.
  }
};

const s3 = new S3Client({
  credentials: {
    accessKeyId: AWS_APP_USER_ACCESS_KEY,
    secretAccessKey: AWS_APP_USER_SECRET_ACCESS_KEY,
  },
  region: AWS_DEFAULT_REGION,
});
async function initiateMultipartUpload(initParams) {
  try {
    const ans = await s3.send(new CreateMultipartUploadCommand(initParams));

    return ans;
  } catch (err) {
    // error handler function here
  }
}

async function UploadPart(body, UploadId, partNumber, filePath) {
  const partParams = {
    Key: filePath,
    Bucket: S3_BUCKET_PRIVATE,
    Body: body,
    UploadId: UploadId,
    PartNumber: partNumber,
  };
  return new Promise(async (resolve, reject) => {
    try {
      let part = await s3.send(new UploadPartCommand(partParams));
      resolve({ PartNumber: partNumber, ETag: part.ETag });
    } catch (error) {
      reject({ partNumber, error });
    }
  });
}

const s3UploadBlobUsingMultipart = async ({
  filePath,
  fileType,
  data,
  size,
  req,
}) => {
  const result = await s3Uploader(filePath, fileType);
  uploadURL = result.requestUrl;
  finalURL = result.fileUrl;
  const keyName = filePath;
  const client = new S3Client({
    credentials: {
      accessKeyId: AWS_APP_USER_ACCESS_KEY,
      secretAccessKey: AWS_APP_USER_SECRET_ACCESS_KEY,
    },
    region: AWS_DEFAULT_REGION,
  });

  const params = {
    Bucket: S3_BUCKET_PRIVATE,
    Key: keyName,
    Body: data,
  };
  // const form = new IncomingForm({ multiples: true });
  // console.log(req);
  // form.parse(req, async (err, fields, files) => {
  //   console.log(files);
  //   if (err) {
  //     console.log(err);
  //   }
  // });
  const key = filePath;

  const file = data; // file to upload

  const fileSize = size; // total size of file
  const chunkSize = 1024 * 1024 * 5; // 5MB defined as each parts size
  const numParts = Math.ceil(fileSize / chunkSize); // number of parts based on the chunkSize specified
  const promise = []; // array to hold each async upload call
  const slicedData = []; // array to contain our sliced data
  let Parts = []; //  to hold all Promise.allSettled resolve and reject response
  let MP_UPLOAD_ID = null; // contain the upload ID to use for all processes
  let FailedUploads = []; // array to populate failed upload
  let CompletedParts = []; //array to hold all completed upload even from retry upload as will be seen later
  let RetryPromise = [];

  // MAIN LOGIC GOES HERE
  try {
    //Initialize multipart upload to S3
    const initParams = {
      Key: filePath,
      Bucket: S3_BUCKET_PRIVATE,
    };
    const initResponse = await initiateMultipartUpload(initParams); // in real life senerio handle error with an if_else

    MP_UPLOAD_ID = initResponse["UploadId"];

    //Array to create upload objects for promise
    for (let index = 1; index <= numParts; index++) {
      let start = (index - 1) * chunkSize;
      let end = index * chunkSize;

      promise.push(
        UploadPart(
          index < numParts ? file.slice(start, end) : file.slice(start),
          MP_UPLOAD_ID,
          index,
          key,
          filePath
        )
      );

      slicedData.push({
        PartNumber: index,
        buffer: Buffer.from(file.slice(start, end + 1)),
      });
    }

    // promise to upload
    Parts = await Promise.allSettled(promise);

    //check if any upload failed
    FailedUploads = Parts.filter((f) => f.status == "rejected");

    try {
      if (FailedUploads.length) {
        for (let i = 0; i < FailedUploads.length; i++) {
          let [data] = slicedData.filter(
            (f) => f.PartNumber == FailedUploads[i].value.PartNumber
          );
          let s = await UploadPart(
            data.buffer,
            MP_UPLOAD_ID,
            data.PartNumber,
            key
          );
          RetryPromise.push(s);
        }
      }

      CompletedParts = Parts.map((m) => m.value);
      CompletedParts.push(...RetryPromise);

      const s3ParamsComplete = {
        Key: filePath,
        Bucket: S3_BUCKET_PRIVATE,
        UploadId: MP_UPLOAD_ID,
        MultipartUpload: {
          Parts: CompletedParts,
        },
      };

      const result = await s3.send(
        new CompleteMultipartUploadCommand(s3ParamsComplete)
      );
    } catch (error) {
      // handle error
      const initParams = {
        Key: filePath,
        Bucket: S3_BUCKET_PRIVATE,
        UploadId: MP_UPLOAD_ID,
      };
      await new AbortMultipartUploadCommand(initParams);
    }
    return { uploadURL, finalURL };
  } catch (error) {
    // Handle error or cancel like below
    const initParams = {
      Key: filePath,
      Bucket: S3_BUCKET_PRIVATE,
      UploadId: MP_UPLOAD_ID,
    };
    await new AbortMultipartUploadCommand(initParams);
  }
  // });
};

/*
 * Get a secure url to access an S3 file.
 * Ensure user is authenticated and has permission before accessing this.
 */
const s3SignedGetUrl = async (filePath) => {
  const S3 = new AWS.S3();
  return new Promise((resolve, reject) => {
    S3.getSignedUrl(
      "getObject",
      {
        Bucket: S3_BUCKET_PRIVATE,
        Key: filePath,
        Expires: ONE_DAY_IN_SEC,
      },
      (err, requestUrl) => {
        if (err) {
          reject(err);
        }
        resolve(requestUrl);
      }
    );
  });
};

const s3Delete = (url) => {
  const S3 = new AWS.S3();

  const params = {
    Bucket: S3_BUCKET_PRIVATE,
    Delete: {
      // required
      Objects: [
        // required
        {
          Key: url, // required
        },
      ],
    },
  };
  S3.deleteObjects(
    params,
    (err, data) =>
      new Promise((resolve, reject) => {
        if (err) return reject(err);
        resolve({ success: "ok", data });
      })
  );
};

/**
 * Fetch the file from the s3 bucket.
 * @param {String} filePath
 * @returns
 */
const fetchFileFromS3 = async ({ filePath, bucket = S3_BUCKET_PRIVATE }) => {
  try {
    const requestUrl = fileUrlFromPath({ filePath, bucket });
    return await axios.get(requestUrl);
  } catch (error) {
    throw error;
  }
};
const fetchLargeFile = async (filePath, res) => {
  try {
    // console.log(filePath);
    const filePath = [
      "demo-upload/car1.jfif",
      "demo-upload/car5.jfif",
      "demo-upload/Files_1681189416515.zip",
    ];
    const S3 = new AWS.S3({ signatureVersion: "v4" });
    // Define the files to include in the ZIP file
    const fileKeys = filePath;
    // Define the S3 bucket name
    const bucketName = S3_BUCKET_PRIVATE;

    // Create a new archiver instance and set the output to be streamed
    const archive = archiver("zip", { zlib: { level: 9 } });
    archive.on("error", (err) => {
      throw err;
    });

    // Add each file to the archive by streaming its contents from S3
    for (const fileKey of fileKeys) {
      const params = { Bucket: bucketName, Key: fileKey };
      archive.append(S3.getObject(params).createReadStream(), {
        name: fileKey,
      });
    }

    // Set the content-disposition header to force download of the ZIP file
    const headers = {
      "Content-Disposition": `attachment; filename="my-zip-file.zip"`,
    };

    // Pipe the ZIP file to the HTTP response object
    archive.pipe(res.set(headers));

    // When the archive is finalized, end the response
    archive.on("finish", () => {
      console.log(`ZIP file created and streamed`);
      res.end();
    });

    // Generate the ZIP file and start streaming it to the HTTP response object
    archive.finalize();
  } catch (error) {
    throw error;
  }
};

module.exports = {
  fileUrlFromPath,
  s3Uploader,
  s3UploadBlob,
  s3SignedGetUrl,
  fetchFileFromS3,
  s3Delete,
  s3UploadBlobUsingMultipart,
  fetchLargeFile,
};
