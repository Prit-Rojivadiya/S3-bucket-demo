const express = require("express");
const assetsController = require("../controllers/assets.controller");

const assetsRouter = express.Router();

assetsRouter.post("/upload-files", assetsController.uploadFiles);
assetsRouter.post(
  "/upload-files-using-multipart",
  assetsController.uploadFilesUsingMultipart
);
assetsRouter.post("/delete-files", assetsController.deleteFiles);
assetsRouter.get("/fetch-file", assetsController.fetchFile);
assetsRouter.get("/fetch-large-file", assetsController.fetchLargeFile);

module.exports = assetsRouter;
