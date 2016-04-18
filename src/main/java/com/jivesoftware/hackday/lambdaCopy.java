package com.jivesoftware.hackday;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Created by gary.schulte on 2/19/16.
 */

public class lambdaCopy implements RequestHandler<S3Event, String> {

    final Pattern path = Pattern.compile("\\/?(\\d+)\\/(\\d+)\\/(\\d+)\\/(\\d+)\\/(.*)");


    public String handleRequest(S3Event s3event, Context context) {
        try {
            S3EventNotificationRecord record = s3event.getRecords().get(0);

            String srcBucket = record.getS3().getBucket().getName();
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getKey()
                    .replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");

            String dstBucket = "hive-" + srcBucket;
            Matcher m = path.matcher(srcKey);

            // Sanity check: validate that source and destination are different
            // buckets.
            if (srcBucket.equals(dstBucket)) {
                System.out
                        .println("Destination bucket must not match source bucket.");
                return "";
            }

            // ensure our path is sensible before copying:

            if (m.find() && m.groupCount() >= 5) {
                String dstKey = "year=" + m.group(1) +
                        "/month=" + m.group(2) +
                        "/day=" + m.group(3) +
                        "/hour=" + m.group(4) +
                        "/" +m.group(5);

                // copying to S3 destination bucket
                System.out.println("Copying: " + srcBucket + "/" + srcKey + " to " + dstBucket + "/" + dstKey);

                // Download the image from S3 into a stream
                AmazonS3 s3Client = new AmazonS3Client();
                S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                        srcBucket, srcKey));

                // copying to S3 destination bucket
                System.out.println("Writing to: " + dstBucket + "/" + dstKey);
                s3Client.copyObject(srcBucket, srcKey, dstBucket, dstKey);
                System.out.println("Successfully copied " + srcBucket + "/"
                        + srcKey + "  to " + dstBucket + "/" + dstKey);
                return "Ok";
            } else {
                System.out.println("failed to create target key from srcBucket: " + srcBucket + " and srcKey: "
                        + srcKey);
                System.out.println("matches group " + m.group());
                return "";
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}