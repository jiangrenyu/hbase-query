package com.hbase.bulkload.txt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkLoadTextDriver {
    static Logger logger = LoggerFactory.getLogger(BulkLoadTextDriver.class);
    public static void main(String[] args) throws Exception {
        final String HBASE_CONFIG_ZOOKEEPER_CLIENT = "hbase.zookeeper.property.clientPort";
        final String HBASE_ZOOKEEPER_CLIENT_PORT = "2181";
        final String HBASE_CONFIG_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
        final String HBASE_ZOOKEEPER_SERVER = "ZJCRNOPGPD31,ZJCRNOPGPD33,ZJCRNOPGPD35";

        Configuration conf = new Configuration();
        conf.set(HBASE_CONFIG_ZOOKEEPER_CLIENT, HBASE_ZOOKEEPER_CLIENT_PORT);
        conf.set(HBASE_CONFIG_ZOOKEEPER_QUORUM, HBASE_ZOOKEEPER_SERVER);
        // fix up phycical memory or virtual memory error
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "4096");
        Configuration hconf = HBaseConfiguration.create(conf);
        
        String inputPath = args[0];
        String outputPath = args[1];
        String tableNameStr = args[2];
        logger.info(">>>>>>>>>>>>>>>>> inputPath: {} <<<<<<<<<<<<<<<<<<<<", inputPath);
        logger.info(">>>>>>>>>>>>>>>>> outputPath: {} <<<<<<<<<<<<<<<<<<<<", outputPath);
        logger.info(">>>>>>>>>>>>>>>>> tableName: {} <<<<<<<<<<<<<<<<<<<<", tableNameStr);
        
        TableName tableName = TableName.valueOf(tableNameStr);
        Connection hbCon = null;
        Table hTable = null;
        RegionLocator regionLocator = null;
        Admin admin = null;
        try {
            Job job = Job.getInstance(hconf, "bulkload job");
            job.setJarByClass(BulkLoadTextDriver.class);
            job.setMapperClass(ReadTextConvertToHFileMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);
            // speculation
            job.setSpeculativeExecution(false);
            job.setReduceSpeculativeExecution(false);
            // in/out format
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(HFileOutputFormat2.class);

            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            hbCon = ConnectionFactory.createConnection(hconf);
            hTable = hbCon.getTable(tableName);
            regionLocator = hbCon.getRegionLocator(tableName);
            admin = hbCon.getAdmin();
            HFileOutputFormat2.configureIncrementalLoad(job, hTable, regionLocator);

            if (job.waitForCompletion(true)) {
//                FsShell shell = new FsShell(conf);
//                try {
//                    shell.run(new String[]{"-chmod", "-R", "777", args[1]});
//                } catch (Exception e) {
//                    logger.error("Couldnt change the file permissions ", e);
//                    throw new IOException(e);
//                }
                //载入到hbase表
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hconf);
                loader.doBulkLoad(new Path(outputPath), admin, hTable, regionLocator); 
                logger.info(">>>>>>>>>>>>>>>>> load success <<<<<<<<<<<<<<<<<<<<");
            } else {
                logger.error("loading failed.");
                System.exit(1);
            }
        }  finally {
            hTable.close();
            regionLocator.close();
            admin.close();
            hbCon.close();
        }
    }
}
