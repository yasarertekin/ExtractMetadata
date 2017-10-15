package com.aws.codestar.projecttemplates.handler;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Handler for requests to Lambda function.
 */
public class ExtractMetadata implements RequestHandler<SNSEvent, Object> {
    private static final AmazonS3 s3 = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());

    private static FileOutputStream fop = null;

    public String handleRequest(final SNSEvent event, final Context context) {
        LambdaLogger logger = context.getLogger();


        String message = event.getRecords().get(0).getSNS().getMessage();
        String bucketName = "serverless-video-transcoded-p";
        String sourceKey = "";
        try {
            sourceKey = URLDecoder.decode(message.replace("/\\+/g", " "), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        logger.log("Source key: " + sourceKey);
        saveFileToFilesystem(bucketName, sourceKey);
        return "ok";
    }

    private void saveFileToFilesystem(String sourceBucket, String sourceKey) {
        String localFileName = sourceKey.split("/")[0];
        File file;
        file = new File("/tmp/" + localFileName);

        try {
            fop = new FileOutputStream(file);

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            GetObjectRequest objectRequest =new GetObjectRequest(sourceBucket,sourceKey);
            S3ObjectInputStream inputStream =s3.getObject(objectRequest).getObjectContent();
            // get the content in bytess
            int curr = -1;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((curr = inputStream.read()) != -1) {
                baos.write(curr);
            }

            fop.write(baos.toByteArray());
            fop.flush();
            fop.close();
            extractMetadata(sourceBucket,sourceKey , localFileName);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fop != null) {
                    fop.close();
                }
            } catch (IOException e) {
                e.printStackTrace();

            }

        }
    }

    private void extractMetadata(String sourceBucket, String sourceKey, String localFilename) {
        try{
            System.out.println("start excution......");
            String[] command = {"/tmp/ffprobe -v quiet -print_format json -show_format /tmp/" + localFilename} ;
            ProcessBuilder p = new ProcessBuilder(command);
            Process p2 = p.start();
            InputStream inputStream = p2.getInputStream();
            String metadataKey = sourceKey.split("/.")[0] + ".json";
            saveMetadataToS3(inputStream,sourceBucket , metadataKey);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    private void saveMetadataToS3(InputStream inputStream, String sourceBucket, String metadataKey) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        PutObjectRequest request = new PutObjectRequest(sourceBucket, metadataKey,inputStream,objectMetadata);
        request.setCannedAcl(CannedAccessControlList.PublicRead);
        s3.putObject(request);

    }
}
