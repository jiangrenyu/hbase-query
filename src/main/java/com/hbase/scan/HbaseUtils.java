package com.hbase.scan;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.List;

public class HbaseUtils {

    private static Admin admin;
    private static Configuration conf;
    private static Connection conn;
    //private static Logger log = LoggerFactory.getLogger(HbaseUtils.class);

    static {
        conf = HBaseConfiguration.create(new Configuration());
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "ZJCRNOPGPD31,ZJCRNOPGPD33,ZJCRNOPGPD35");
        try {
            conn = ConnectionFactory.createConnection(conf);
            System.out.println("conn-----------------------------------------:" + conn);
            admin = conn.getAdmin();
            //log.info("初始化hbase连接成功...........");
        } catch (IOException e) {
            //log.info("初始化hbase连接异常...........");
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {

//        String imsi = "460110777619208";//args[0];
//        String start_time = "2019-03-13 06:50:34.652";//args[1];
//        String end_time = "2019-03-13 09:50:34.652"; //args[2];
//        String start_rowKey = getRow(start_time, imsi);
//        String end_rowKey = getRow(end_time, imsi);

//        String table = args[0];
//        String start_rowKey = args[1];
//        String end_rowKey = args[2];
//
//        System.out.println("表: " + table);
//        System.out.println("start_rowKey: " + start_rowKey);
//        System.out.println("end_rowKey: " + end_rowKey);
//
//            long start = System.currentTimeMillis();
//            scan(table, start_rowKey, end_rowKey);
//            long end = System.currentTimeMillis();
//            System.out.println("查询耗时: " + (end - start) + " :ms");
        System.out.println("===================================================================");

        String table = args[0];
        String rowKey = args[1];
        long start = System.currentTimeMillis();
        System.out.println(getDataByRow(table, rowKey));
        long end = System.currentTimeMillis();
        System.out.println("查询耗时: " + (end - start) + " :ms");
        //}
    }

    public static String getDataByRow(String tableName, String rowKey) {
        TableName table = TableName.valueOf(tableName);

        StringBuffer stringBuffer = new StringBuffer();
        try {
            if (!admin.tableExists(table)) {
                //log.error("表不存在{}", tableName);
                System.exit(-1);
            }

            Table htable = conn.getTable(table);
            Get get = new Get(rowKey.getBytes());

            Result result = htable.get(get);
            List<Cell> cellList = result.listCells();
            for (Cell cell : cellList) {
                stringBuffer.append(new String(CellUtil.cloneValue(cell)) + ",");
            }
            return stringBuffer.deleteCharAt(stringBuffer.length() - 1).toString();
        } catch (IOException e) {
            //log.error("查询异常！！！！！！！！");
            e.printStackTrace();
        } finally {
            try {
                admin.close();
                conn.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        return "";
    }

    public static void scan(String tableName, String start_row, String end_row) {
        System.out.println("===================================================开始查询......................................... ......");
        int i = 0;

        ResultScanner resultScanner = null;
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));// new HTable(conf, Bytes.toBytes(tableName));
            Scan scan = new Scan();
            scan.withStartRow(start_row.getBytes());
            scan.withStopRow(end_row.getBytes());

            for (Result rt : resultScanner) {
                String value = getLine(rt);
                System.out.println("数据: " + value);
                ++i;
            }
            System.out.println("查询数据行数: " + i);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getLine(Result result) {
        StringBuffer stringBuffer = new StringBuffer();
        try {
            for (Cell cell : result.rawCells()) {
                String value;
                value = new String(CellUtil.cloneValue(cell), "UTF-8");
                stringBuffer.append(value + ",");
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return stringBuffer.deleteCharAt(stringBuffer.length() - 1).toString();
    }

    public static void scan1(String tableName, String start_row, String end_row) {
        System.out.println("===================================================开始查询......................................... ......");
        int i = 0;

        ResultScanner resultScanner = null;
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));// new HTable(conf, Bytes.toBytes(tableName));
            Scan scan = new Scan();
            scan.withStartRow(start_row.getBytes());
            scan.withStopRow(end_row.getBytes());

            resultScanner = table.getScanner(scan);

            for (Result rt : resultScanner) {
                List<Cell> cells = rt.listCells();
                for (Cell cell : cells) {
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                }
            }
            System.out.println("查询数据行数: " + i);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getRow(String time1, String imsi1) {
        String imsi = imsi1;
        String callStartTime = time1;
        if (StringUtils.isEmpty(imsi) || StringUtils.isEmpty(callStartTime)) {
            return "";
        }
        callStartTime = callStartTime.replaceAll(" ", "").replaceAll(":", "").replaceAll("-", "").replaceAll("\\.", "");
        String rowKeyPrefix = DigestUtils.md5Hex(imsi);
        rowKeyPrefix = rowKeyPrefix.substring(0, 4);
        // remove '46011'
        if (imsi.length() == 15 && imsi.startsWith("460")) {
            imsi = imsi.substring(5);
        } else {
            return "";
        }
        if (callStartTime.length() == 17 && callStartTime.startsWith("20")) {
            callStartTime = callStartTime.substring(2);
        } else {
            return "";
        }
        return rowKeyPrefix + imsi + callStartTime;
    }
}

