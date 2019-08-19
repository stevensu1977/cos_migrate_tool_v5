package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.ListPartsRequest;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos.model.UploadResult;
import com.qcloud.cos.transfer.PersistableUpload;
import com.qcloud.cos.transfer.Transfer.TransferState;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.transfer.TransferProgress;
import com.qcloud.cos.transfer.Upload;
import com.qcloud.cos.utils.Md5Utils;
import com.qcloud.cos_migrate_tool.config.CommonConfig;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.record.RecordDb.QUERY_RESULT;
import com.qcloud.cos_migrate_tool.record.RecordElement;

public abstract class Task implements Runnable {
    private Semaphore semaphore;
    public static final Logger log = LoggerFactory.getLogger(Task.class);
    protected static Semaphore mutex = new Semaphore(1);

    protected TransferManager smallFileTransfer;
    protected TransferManager bigFileTransfer;
    protected long smallFileThreshold;
    private RecordDb recordDb;
    protected CommonConfig config;
    QUERY_RESULT query_result;

    public Task(Semaphore semaphore, CommonConfig config, TransferManager smallFileTransfer,
            TransferManager bigFileTransfer, RecordDb recordDb) {
        super();
        this.semaphore = semaphore;
        this.config = config;
        this.smallFileTransfer = smallFileTransfer;
        this.bigFileTransfer = bigFileTransfer;
        this.smallFileThreshold = config.getSmallFileThreshold();
        this.recordDb = recordDb;
    }

    public boolean isExist(RecordElement recordElement) {
        query_result = recordDb.queryRecord(recordElement);
        if (query_result == RecordDb.QUERY_RESULT.ALL_EQ) {
            String printMsg = String.format("[skip] task_info: %s", recordElement.buildKey());
            System.out.println(printMsg);
            log.info("skip! task_info: [key: {}], [value: {}]", recordElement.buildKey(), recordElement.buildValue());
            return true;
        }

        return false;
    }

    public void saveRecord(RecordElement recordElement) {
        recordDb.saveRecord(recordElement);
    }

    public void saveRequestId(String key, String requestId) {
        recordDb.saveRequestId(key, requestId);
    }

    private void printTransferProgress(TransferProgress progress, String key) {
        long byteSent = progress.getBytesTransferred();
        long byteTotal = progress.getTotalBytesToTransfer();
        double pct = 100.0;
        if (byteTotal != 0) {
            pct = progress.getPercentTransferred();
        }
        String printMsg = String.format(
                "[UploadInProgress] [key: %s] [byteSent/ byteTotal/ percentage: %d/ %d/ %.2f%%]", key, byteSent,
                byteTotal, pct);
        log.info(printMsg);
        System.out.println(printMsg);
    }

    public String showTransferProgressAndGetRequestId(Upload upload, boolean multipart, String key, long mtime)
            throws InterruptedException {
        boolean pointSaveFlag = false;
        long printCount = 0;
        TransferProgress progress = upload.getProgress();
        do {
            ++printCount;
            Thread.sleep(100);

            long byteSent = progress.getBytesTransferred();
            if (printCount % 20 == 0) {
                printTransferProgress(progress, key);
            }
            if (multipart && byteSent > 0 && !pointSaveFlag) {

                PersistableUpload persistableUploadInfo = upload.getResumeableMultipartUploadId();
                String multipartUploadId = null;
                if (persistableUploadInfo != null) {
                    multipartUploadId = persistableUploadInfo.getMultipartUploadId();
                    if (multipartUploadId != null) {
                        pointSaveFlag = this.recordDb.updateMultipartUploadSavePoint(
                                persistableUploadInfo.getBucketName(), persistableUploadInfo.getKey(),
                                persistableUploadInfo.getFile(), mtime, persistableUploadInfo.getPartSize(),
                                persistableUploadInfo.getMutlipartUploadThreshold(),
                                persistableUploadInfo.getMultipartUploadId());
                        if (pointSaveFlag) {
                            log.info("save point success for multipart upload, key: {}", key);
                        } else {
                            log.error("save point failed for multipart upload, key: {}", key);
                        }
                    }

                }
            }

        } while (upload.isDone() == false);
        // 结束后在打印下进度
        printTransferProgress(progress, key);
        // 传输完成, 删除断点信息
        if (upload.getState() == TransferState.Completed && pointSaveFlag) {
            PersistableUpload persistableUploadInfo = upload.getResumeableMultipartUploadId();
            String multipartUploadId = null;
            if (persistableUploadInfo != null) {
                multipartUploadId = persistableUploadInfo.getMultipartUploadId();
                if (multipartUploadId != null) {
                    boolean deleteFlag = this.recordDb.deleteMultipartUploadSavePoint(
                            persistableUploadInfo.getBucketName(), persistableUploadInfo.getKey(),
                            persistableUploadInfo.getFile(), mtime, persistableUploadInfo.getPartSize(),
                            persistableUploadInfo.getMutlipartUploadThreshold());
                    if (deleteFlag) {
                        log.info("delete point success for multipart upload, key: {}", key);
                    } else {
                        log.info("delete point failed for multipart upload, key: {}", key);
                    }
                }
            }
        }
        UploadResult uploadResult = upload.waitForUploadResult();
        return uploadResult.getRequestId();
    }

    private boolean isMultipartUploadIdValid(String bucketName, String cosKey, String uploadId) {
        ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName, cosKey, uploadId);
        try {
            this.bigFileTransfer.getCOSClient().listParts(listPartsRequest);
            return true;
        } catch (CosServiceException cse) {
            return false;
        }

    }

    private String uploadBigFile(PutObjectRequest putObjectRequest) throws InterruptedException {

        // if cloudVendor is tencent
        if (this.config.getCloudVendor().equalsIgnoreCase("tencent")) {

            String bucketName = putObjectRequest.getBucketName();
            String cosKey = putObjectRequest.getKey();
            String localPath = putObjectRequest.getFile().getAbsolutePath();
            long mtime = putObjectRequest.getFile().lastModified();
            long partSize = this.bigFileTransfer.getConfiguration().getMinimumUploadPartSize();
            long mutlipartUploadThreshold = this.bigFileTransfer.getConfiguration().getMultipartUploadThreshold();

            String multipartId = this.recordDb.queryMultipartUploadSavePoint(bucketName, cosKey, localPath, mtime,
                    partSize, mutlipartUploadThreshold);
            Upload upload = null;
            // 如果multipartId不为Null, 则表示存在断点, 使用续传.
            if (multipartId != null && isMultipartUploadIdValid(bucketName, cosKey, multipartId)) {
                PersistableUpload persistableUpload = new PersistableUpload(bucketName, cosKey, localPath, multipartId,
                        partSize, mutlipartUploadThreshold);
                upload = this.bigFileTransfer.resumeUpload(persistableUpload);
            } else {
                upload = this.bigFileTransfer.upload(putObjectRequest);
            }
            return showTransferProgressAndGetRequestId(upload, true, cosKey, mtime);

        }

        // if cloudVendor is aws
        if (this.config.getCloudVendor().equalsIgnoreCase("AWS")) {

            String keyName = putObjectRequest.getKey();
            if (keyName.startsWith("/")) {
                keyName = keyName.substring(1);

            }

            try {
                File f = new File(putObjectRequest.getFile().getAbsolutePath());
                FileInputStream fin = new FileInputStream(f);
                com.amazonaws.services.s3.model.ObjectMetadata meta = new com.amazonaws.services.s3.model.ObjectMetadata();

                meta.setContentType(putObjectRequest.getMetadata().getContentType());
                meta.setContentLength(putObjectRequest.getMetadata().getContentLength());
                meta.setContentMD5(putObjectRequest.getMetadata().getContentMD5());

                com.amazonaws.services.s3.transfer.TransferManager xfer_mgr = AWSS3Client
                        .buildTransferManager(this.config);
                // com.amazonaws.services.s3.transfer.Upload xfer =
                // xfer_mgr.upload(putObjectRequest.getBucketName(), keyName,
                // f);
                com.amazonaws.services.s3.transfer.Upload xfer = xfer_mgr.upload(putObjectRequest.getBucketName(),
                        keyName, fin, meta);

                XferMgrProgress.showTransferProgress(xfer);
                XferMgrProgress.waitForCompletion(xfer);

            } catch (com.amazonaws.AmazonServiceException e) {
                System.err.println(e.getErrorMessage());
                System.exit(1);

            } catch (com.amazonaws.AmazonClientException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            } catch (java.io.FileNotFoundException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
            return putObjectRequest.getBucketName() + "/" + keyName;
        }

        return "No task";
    }

    private String uploadSmallFile(PutObjectRequest putObjectRequest) throws InterruptedException {

        // if cloudVendor is tencent
        if (this.config.getCloudVendor().equalsIgnoreCase("tencent")) {
            Upload upload = smallFileTransfer.upload(putObjectRequest);
            return showTransferProgressAndGetRequestId(upload, false, putObjectRequest.getKey(),
                    putObjectRequest.getFile().lastModified());
        }

        // if cloudVendor is aws
        if (this.config.getCloudVendor().equalsIgnoreCase("AWS")) {

            log.debug("invoke uploadSmallFile");
            log.debug(putObjectRequest.getFile().getAbsolutePath());

            String keyName = putObjectRequest.getKey();
            if (keyName.startsWith("/")) {
                keyName = keyName.substring(1);

            }

            log.debug(putObjectRequest.getKey());
            log.debug(keyName);

            try {

                File f = new File(putObjectRequest.getFile().getAbsolutePath());
                FileInputStream fin = new FileInputStream(f);
                com.amazonaws.services.s3.model.ObjectMetadata meta = new com.amazonaws.services.s3.model.ObjectMetadata();

                meta.setContentType(putObjectRequest.getMetadata().getContentType());
                meta.setContentLength(putObjectRequest.getMetadata().getContentLength());
                meta.setContentMD5(putObjectRequest.getMetadata().getContentMD5());

                com.amazonaws.services.s3.transfer.TransferManager xfer_mgr = AWSS3Client
                        .buildTransferManager(this.config);
                // com.amazonaws.services.s3.transfer.Upload xfer =
                // xfer_mgr.upload(putObjectRequest.getBucketName(),
                // keyName, f);
                com.amazonaws.services.s3.transfer.Upload xfer = xfer_mgr.upload(putObjectRequest.getBucketName(),
                        keyName, fin, meta);
                XferMgrProgress.showTransferProgress(xfer);
                XferMgrProgress.waitForCompletion(xfer);
            } catch (com.amazonaws.AmazonServiceException e) {
                System.err.println(e.getErrorMessage());
                System.exit(1);
            } catch (com.amazonaws.AmazonClientException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            } catch (java.io.FileNotFoundException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }

            return "Completed";
        }

        return "No task";

    }

    public String uploadFile(String bucketName, String cosPath, File localFile, StorageClass storageClass,
            boolean entireMd5Attached, ObjectMetadata objectMetadata) throws Exception {
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, cosPath, localFile);
        putObjectRequest.setStorageClass(storageClass);

        if (entireMd5Attached) {
            String md5 = Md5Utils.md5Hex(localFile);
            objectMetadata.addUserMetadata("md5", md5);
        }

        if (config.getEncryptionType().equals("sse-cos")) {
            objectMetadata.setServerSideEncryption("AES256");
        }

        putObjectRequest.setMetadata(objectMetadata);
        int retryTime = 0;
        final int maxRetry = 5;

        while (retryTime < maxRetry) {
            try {
                if (localFile.length() >= smallFileThreshold) {
                    return uploadBigFile(putObjectRequest);

                } else {
                    return uploadSmallFile(putObjectRequest);

                }
            } catch (Exception e) {
                log.warn("upload failed, ready to retry. retryTime:" + retryTime, e);
                ++retryTime;
                if (retryTime >= maxRetry) {
                    throw e;
                } else {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(200, 1000));
                }
            }
        }
        return null;
    }

    public abstract void doTask();

    private void checkTimeWindows() throws InterruptedException {
        int timeWindowBegin = config.getTimeWindowBegin();
        int timeWindowEnd = config.getTimeWindowEnd();
        while (true) {
            DateTime dateTime = DateTime.now();
            int minuteOfDay = dateTime.getMinuteOfDay();
            if (minuteOfDay >= timeWindowBegin && minuteOfDay <= timeWindowEnd) {
                return;
            }

            if (mutex.tryAcquire()) {
                String printTips = String.format("currentTime %s, wait next time window [%02d:%02d, %02d:%02d]",
                        dateTime.toString("yyyy-MM-dd HH:mm:ss"), timeWindowBegin / 60, timeWindowBegin % 60,
                        timeWindowEnd / 60, timeWindowEnd % 60);
                System.out.println(printTips);
                System.out.println("---------------------------------------------------------------------");
                log.info(printTips);
                Thread.sleep(60000);
                mutex.release();
            } else {
                Thread.sleep(60000);
            }
        }
    }

    public void run() {
        try {
            checkTimeWindows();
            doTask();
        } catch (InterruptedException e) {
            log.error("task is interrupted", e);
        } catch (Exception e) {
            log.error("unknown exception occur", e);
        } finally {
            semaphore.release();
        }
    }
}
