package com.hbase.bulkload.txt;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class ReadTextConvertToHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    static Logger logger = LoggerFactory.getLogger(ReadTextConvertToHFileMapper.class);

    public static void main(String args[]) {
        System.out.println(new ReadTextConvertToHFileMapper().getTime(System.currentTimeMillis()).length());
    }

    private String getTime(long time) {
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        return format.format(time);
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueStr = value.toString();
        if (valueStr.contains("time_stamp")) { // first line is header
            return;
        }
        String[] valueStrSplit = valueStr.split("~", -1);
        // index = 35, CALL_START_TIME
        // index = 26, IMSI;
        if (valueStrSplit.length < 35) {
            return; //TODO record invalid line
        }
        String imsi = valueStrSplit[26];
        String callStartTime = valueStrSplit[35];
        if (StringUtils.isEmpty(imsi) || StringUtils.isEmpty(callStartTime)) {
            // IMSI or CALL_START_TIME is empty, then return
            return; //TODO record invalid line
        }
        callStartTime = callStartTime.replaceAll(" ", "").replaceAll(":", "").replaceAll("-", "").replaceAll("\\.", "");
        String family = "cf1";
        String rowKeyPrefix = DigestUtils.md5Hex(imsi);
        rowKeyPrefix = rowKeyPrefix.substring(0, 4);
        // remove '46011'
        if (imsi.length() == 15 && imsi.startsWith("460")) {
            imsi = imsi.substring(5);
        } else {
            return; //TODO record invalid line
        }
        if (callStartTime.length() == 17 && callStartTime.startsWith("20")) {
            callStartTime = callStartTime.substring(2);
        } else {
            return; //TODO record invalid line
        }
        //callStartTime = getTime(System.currentTimeMillis());

        String hKey = rowKeyPrefix + imsi + callStartTime;
        logger.info("rowKey== " + hKey);
        final byte[] rowKey = Bytes.toBytes(hKey);
        final ImmutableBytesWritable HKey = new ImmutableBytesWritable(rowKey);
        Put hPut = null;
        for (int i = 0; i < valueStrSplit.length; i++) {
            if (StringUtils.EMPTY.equals(valueStrSplit[i])) {
                continue;
            }
            byte[] column = HColumnText.cloumnList.get(i);
            String hvalue = valueStrSplit[i];
            byte[] cell = Bytes.toBytes(hvalue);
            if (hPut == null) {
                hPut = new Put(rowKey);
            }
            hPut.addColumn(Bytes.toBytes(family), column, cell);
        }
        if (hPut != null) {
            context.write(HKey, hPut);
        }
    }
}