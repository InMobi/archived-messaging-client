package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class TestCollectorStreamWithEmptyFiles {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);
  private Cluster cluster;
  private String[] files = new String[] {TestUtil.files[0]};
  private String[] emptyfiles = new String[] {TestUtil.files[1]};
  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(101);

  private Path collectorDir;
  private Path streamsLocalDir;
  private Configuration conf = new Configuration();
  private FileSystem fs;
  int consumerNumber;

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
  	consumerNumber = 1;
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, emptyfiles, 0);
    collectorDir = DatabusUtil.getCollectorStreamDir(
        new Path(cluster.getRootDir()), testStream,
        collectorName);
    streamsLocalDir = DatabusUtil.getStreamDir(StreamType.LOCAL,
        new Path(cluster.getRootDir()), testStream);
    fs = FileSystem.get(cluster.getHadoopConf());
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testReadFromStart() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    PartitionReader preader = new PartitionReader(partitionId, null, conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[0]), 5, 1000,
        DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.start();
    
    // Test reading from next file
    while (buffer.remainingCapacity() != 1) {
      Thread.sleep(10);
    }

    // test waiting for data in current scribe file by sleep for some more time
    Thread.sleep(20);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    String dataFile = TestUtil.files[2];
    
    TestUtil.setUpCollectorDataFiles(fs, collectorDir, dataFile);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[0]), 1,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(dataFile), 1,
        0, 100, partitionId, buffer, true);
    
    // Test the path for current file getting created late.
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    String emptyFile = TestUtil.files[3];
    dataFile = TestUtil.files[4];
    TestUtil.setUpEmptyFiles(fs, collectorDir, emptyFile);
    Thread.sleep(20);
    fs.delete(new Path(collectorDir, emptyfiles[0]), true);
    fs.delete(new Path(collectorDir, emptyFile), true);
    Thread.sleep(50);
    TestUtil.setUpCollectorDataFiles(fs, collectorDir, dataFile);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(dataFile),
        1, 0, 100, partitionId, buffer, true);
    
    //Test the path for next higher entry
    emptyFile = TestUtil.files[5];
    dataFile = TestUtil.files[6];
    TestUtil.setUpEmptyFiles(fs, collectorDir, emptyFile);
    Thread.sleep(20);
    fs.delete(new Path(collectorDir, emptyFile), true);
    TestUtil.setUpCollectorDataFiles(fs, collectorDir, dataFile);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(dataFile), 1,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    preader.close();
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 400);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 400);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

}
