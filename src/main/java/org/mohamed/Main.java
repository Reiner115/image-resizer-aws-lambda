package org.mohamed;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import net.coobird.thumbnailator.Thumbnails;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import javax.imageio.ImageIO;

public class Main {
    public static void main(String[] args) throws IOException {

    }

    final static String accessKey  = System.getenv("accessKey");
    final static String secretKey = System.getenv("secretKey");
    final String bucketToPutOn = System.getenv("bucketToPutOn");
    final double outputQuality = Double.parseDouble(System.getenv("outputQuality"));
    final double scale = Double.parseDouble(System.getenv("scale"));
    public String handleRequest(final S3Event input, final Context context) throws IOException {

        LambdaLogger logger = context.getLogger();

        logger.log("starting handleRequest lambda function");


        //create aws credentials
        final AWSCredentials credentials = new BasicAWSCredentials(accessKey,secretKey);

        for(S3EventNotification.S3EventNotificationRecord event :  input.getRecords() ) {
            //read filename and bucket name from input
            String fileName = event.getS3().getObject().getKey();
            String bucketName = event.getS3().getBucket().getName();
            logger.log("file name is : " + fileName + ", bucket name is : " + bucketName);
            if (!isImage(fileName)) {
                logger.log("the file being processed is not an image , returning");
                continue;
            }

            InputStream objectData = null;
            try {
                logger.log("getting file from S3 bucket");
                final AmazonS3 s3Client = new AmazonS3Client(credentials);
                S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
                objectData = object.getObjectContent();

                logger.log("reducing file size");
                byte[] thumbnailBytes = getReducedFile(objectData, getFileExtension(fileName));
                ObjectMetadata objectMetadata = new ObjectMetadata();
                fileName = "mini-" + fileName;
                logger.log("writing file to " + bucketToPutOn + " bucket");
                s3Client.putObject(bucketToPutOn, fileName, new ByteArrayInputStream(thumbnailBytes), objectMetadata);
                objectData.close();

            } catch (IOException e) {
                logger.log(e.getMessage());
                logger.log(e.getCause().toString());
                logger.log(e.getStackTrace().toString());

                try {
                    objectData.close();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                throw new RuntimeException(e);
            }
        }
        return "done successfully";
    }


    public static boolean isImage(String filePath) {
        String extension = getFileExtension(filePath).toLowerCase();
        return extension.equals("jpg") || extension.equals("jpeg") || extension.equals("png") || extension.equals("gif");
    }
    public static String getFileExtension(String filePath) {
        int dotIndex = filePath.lastIndexOf(".");
        if (dotIndex > 0 && dotIndex < filePath.length() - 1) {
            return filePath.substring(dotIndex + 1);
        }
        return "";
    }

    private  byte[] getReducedFile(InputStream file , String fileType ){

        try {
            BufferedImage thumbnail =Thumbnails.of(file)
                    .scale(scale)
                    .outputQuality(outputQuality).asBufferedImage();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(thumbnail, fileType , baos);

            return baos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}