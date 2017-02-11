package com.jivesoftware.hackday;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.apache.commons.dbutils.DbUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @since 2/10/17.
 */
public class DevonAthena implements RequestHandler<S3Event, String> {

    final Pattern path = Pattern.compile("\\/?(\\d+)\\/(\\d+)\\/(\\d+)\\/(\\d+)\\/(.*)");

    static final String partitionTemplate =
            "ALTER TABLE devonathena.logs_raw ADD IF NOT EXISTS PARTITION " +
                    "(year = %1$s, month = %2$s, day = %3$s, hour = %4$s) " +
                    "LOCATION 's3://devon-hackday/logs/%1$s/%2$s/%3$s/%4$s'";


    @Override
    public String handleRequest(S3Event s3Event, Context context) {

        S3EventNotification.S3EventNotificationRecord record = s3Event.getRecords().get(0);

        String srcBucket = record.getS3().getBucket().getName();
        // Object key may have spaces or unicode non-ASCII characters.
        String srcKey = record.getS3().getObject().getKey()
                .replace('+', ' ');
        try {
            srcKey = URLDecoder.decode(srcKey, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        Matcher m = path.matcher(srcKey);

        // ensure our path is sensible before copying:
        if (m.find() && m.groupCount() >= 5) {
            // add partition for this file if it hasn't already been added:
            addPartition(m.group(1), m.group(2), m.group(3), m.group(4));
        }

        return "OK";

    }

    private void addPartition(String year, String month, String day, String hour) {
        // just in case:
        try {
            Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        Connection conn = null;
        Statement st = null;
        String sql = String.format(partitionTemplate, year, month, day, hour);
        try {
            Properties props = new Properties();
            props.put("user", System.getenv("key"));
            props.put("password", System.getenv("sec"));
            props.put("s3_staging_dir","s3://devon-hackday/tmp");
            conn = DriverManager.getConnection("jdbc:awsathena://athena.us-west-2.amazonaws.com:443", props);
            st = conn.createStatement();

            st.execute(sql);

        } catch (SQLException e) {
            throw new RuntimeException(sql, e);
        } finally {
            DbUtils.closeQuietly(st);
            DbUtils.closeQuietly(conn);
        }
    }
}
