package com.qcloud.cos_migrate_tool.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.auth.BasicAWSCredentials;


import com.qcloud.cos_migrate_tool.config.CommonConfig;


public class AWSS3Client {

    public static final Logger log = LoggerFactory.getLogger(AWSS3Client.class);

    /* this AWSS3Client builder*/
    public static TransferManager buildTransferManager(CommonConfig config){
       
        log.info("AWS Region: "+config.getRegion());
        BasicAWSCredentials awsCreds = new com.amazonaws.auth.BasicAWSCredentials(config.getAk(),config.getSk());
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                        .withCredentials(new com.amazonaws.auth.AWSStaticCredentialsProvider(awsCreds))
                        .withRegion(config.getRegion())
                        .build();

        TransferManager xfer_mgr = TransferManagerBuilder
                .standard()
                .withS3Client(amazonS3)
                .withMultipartUploadThreshold(config.getSmallFileThreshold())
                .build();

        return xfer_mgr;
    
    }
}
