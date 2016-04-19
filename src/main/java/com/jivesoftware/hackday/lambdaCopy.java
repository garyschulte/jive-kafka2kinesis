package com.jivesoftware.hackday;

import java.io.*;
import java.net.URLDecoder;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.jivesoftware.community.cloudalytics.event.JsonAvroCloner;
import com.jivesoftware.community.cloudalytics.event.avro.AvroEvent;
import com.jivesoftware.community.cloudalytics.event.jsonschema.EventDocument;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.compress.utils.IOUtils;

/**
 * Created by gary.schulte on 2/19/16.
 *
 * quick and dirty lambda function that reads cloudalytic json events and
 * writes to s3 bucket as avro
 */

public class lambdaCopy implements RequestHandler<S3Event, String> {

    final Pattern path = Pattern.compile("\\/?(\\d+)\\/(\\d+)\\/(\\d+)\\/(\\d+)\\/(.*)");
    ObjectMapper mapper = new ObjectMapper();
    ObjectMapper looseMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    AmazonS3 s3Client = new AmazonS3Client();

    int numRead = 0, jsonErrors = 0, avroErrors = 0, unrecognizedPropertyErrors = 0;


    public String handleRequest(S3Event s3event, Context context) {
        BufferedReader br = null;
        ByteArrayOutputStream baos = null;
        DataFileWriter<AvroEvent> s3AvroWriter = null;

        try {
            S3EventNotificationRecord record = s3event.getRecords().get(0);

            String srcBucket = record.getS3().getBucket().getName();
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getKey()
                    .replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");

            String dstBucket = "avro-" + srcBucket;
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
                        "/" + m.group(5);

                // copying to S3 destination bucket
                System.out.println("Copying: " + srcBucket + "/" + srcKey + " to " + dstBucket + "/" + dstKey);

                S3Object s3SourceObject = s3Client.getObject(new GetObjectRequest(
                        srcBucket, srcKey));
                br = new BufferedReader(new InputStreamReader(s3SourceObject.getObjectContent()));


                // target output
                baos = new ByteArrayOutputStream();
                s3AvroWriter = setupSnappyAvroStream(baos);
                String doc = null;
                while (null != (doc = br.readLine())){
                    numRead++;
                    try {
                        EventDocument evtDoc = parsePOJO(doc);
                        AvroEvent avroEvent = JsonAvroCloner.clone(evtDoc);
                        s3AvroWriter.append(avroEvent);
                    } catch (POJOException pjex) {
                        jsonErrors++;
                    } catch (IOException ioex) {
                        avroErrors++;
                        String errmsg = ioex.getMessage();
                        int firstCR = errmsg.indexOf("\n");
                        System.out.println(errmsg.substring(0, (firstCR > 0) ? firstCR : errmsg.length()));
                    }
                }

                writeToS3(baos, dstBucket, dstKey);
                System.out.println(String.format("%s, successfully transformed %s/%s to %s/%s"
                        , new Date()
                        , srcBucket
                        , srcKey
                        , dstBucket
                        , dstKey));

                return "Ok";
            } else {
                System.out.println("failed to create target key from srcBucket: " + srcBucket + " and srcKey: "
                        + srcKey);
                System.out.println("matches group " + m.group());
                return "";
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            System.out.println(String.format("Processed %s docs, %s jackson errors, %s unrecognized property errors, %s avro errors"
                    , numRead
                    , jsonErrors
                    , unrecognizedPropertyErrors
                    , avroErrors));
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(baos);
            IOUtils.closeQuietly(s3AvroWriter);
        }
    }


    EventDocument parsePOJO(String doc) throws POJOException {

        EventDocument jsonDoc = null;
        try {
            try {
                jsonDoc = mapper.readValue(doc, EventDocument.class);
            } catch (UnrecognizedPropertyException upex) {
                System.err.println(String.format("JACKSON at %s for \nunmapped property %s at %s ", new Date(), upex.getUnrecognizedPropertyName(), upex.getPathReference()));
                //TODO: stats in lambda context
                unrecognizedPropertyErrors++;
                //upex.printStackTrace();
                jsonDoc = looseMapper.readValue(doc, EventDocument.class);
            }
            return jsonDoc;
        } catch (IOException ioex) {
            throw new POJOException(ioex);
        }
    }

    DataFileWriter<AvroEvent> setupSnappyAvroStream(ByteArrayOutputStream baos) throws IOException {
        DatumWriter<AvroEvent> writer = new SpecificDatumWriter<>(AvroEvent.class);
        DataFileWriter<AvroEvent> avroOut = new DataFileWriter<>(writer)
                .setCodec(CodecFactory.snappyCodec())
                .create(AvroEvent.getClassSchema(), baos);
        return avroOut;
    }


    void writeToS3(ByteArrayOutputStream baos, String dstBucket, String dstKey) {
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectMetadata om = new ObjectMetadata();
        om.setContentType("avro/binary");
        om.setContentLength(baos.size());

        PutObjectRequest putReq = new PutObjectRequest(dstBucket, dstKey, bais, om);
        PutObjectResult s3TargetResult = s3Client.putObject(putReq);

    }

    public static class POJOException extends Exception{
        POJOException(IOException ioex) {
            this.initCause(ioex);
        }
    }
}