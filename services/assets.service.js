const {
  s3UploadBlob,
  s3Delete,
  s3SignedGetUrl,
  s3UploadBlobUsingMultipart,
  fetchLargeFile,
} = require("../utils/aws");
const { Readable } = require("stream");

module.exports = {
  async uploadFiles(files) {
    try {
      const uploadUrls = [];
      for (const file of Object.keys(files)) {
        const fileToUpload = files[file];
        const { name, mimetype, data, size } = fileToUpload;
        console.log(fileToUpload);
        const filePath = `demo-upload/${name}`;
        const fileType = mimetype;
        const url = await s3UploadBlob({
          filePath,
          fileType,
          data,
          size,
        });
        uploadUrls.push(url);
      }
      console.log("Files uploaded successfully");
      return uploadUrls;
    } catch (error) {
      console.log(error);
    }
  },
  async uploadFilesUsingMultipart(files, req) {
    try {
      const uploadUrls = [];
      for (const file of Object.keys(files)) {
        const fileToUpload = files[file];
        const { name, mimetype, data, size } = fileToUpload;
        console.log(fileToUpload);
        const filePath = `demo-upload/${name}`;
        const fileType = mimetype;
        const url = await s3UploadBlobUsingMultipart({
          filePath,
          fileType,
          data,
          size,
          req,
        });
        uploadUrls.push(url);
      }
      console.log("Files uploaded successfully");
      return uploadUrls;
    } catch (error) {
      console.log(error);
    }
  },
  async deleteFiles(filePaths) {
    try {
      for (const filePath of filePaths) {
        const data = await s3Delete(filePath);
        console.log("file deleted successfully");
      }
    } catch (error) {
      console.log(error);
    }
  },
  async fetchFile(filePath) {
    try {
      const data = await s3SignedGetUrl(filePath);
      return data;
    } catch (error) {
      console.log(error);
    }
  },
  async fetchLargeFile(filePath, res) {
    try {
      const data = await fetchLargeFile(filePath, res);
      return data;
    } catch (error) {
      console.log(error);
    }
  },
};
