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
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class TestPartitionReaderMovingFilesFromLocalStream {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(149);
  private Cluster cluster;
  private PartitionReader preader;
  private FileSystem fs;
  private Path collectorDir;
  private Path streamsLocalDir;
  private Configuration conf = new Configuration();

  private String[] files = new String[] {TestUtil.files[1],
      TestUtil.files[2], TestUtil.files[3], TestUtil.files[4],
      TestUtil.files[5], TestUtil.files[6], TestUtil.files[7],
      TestUtil.files[8]};
  private Path[] databusFiles = new Path[8];
  int consumerNumber;

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
  	consumerNumber = 1;
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, databusFiles, 4);
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
  public void testLocalStreamFileMoved() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, 1000, DataEncodingType.BASE64, prMetrics);

    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());

    preader.start();
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    fs.delete(databusFiles[0], true);
    fs.delete(databusFiles[1], true);
    fs.delete(databusFiles[2], true);
    databusFiles[4] = TestUtil.moveFileToStreamLocal(fs, testStream,
        collectorName, cluster, collectorDir, files[4]);
    databusFiles[5] = TestUtil.moveFileToStreamLocal(fs, testStream,
        collectorName, cluster, collectorDir, files[5]);

    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2, 0, 50, partitionId, buffer, true);

    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    Assert.assertEquals(((CollectorReader)preader.getReader()).
        getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2, 50, 50, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[3]), 4, 0, 100, partitionId, buffer, true);

    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    Assert.assertEquals(((CollectorReader)preader.getReader()).
        getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    fs.delete(databusFiles[3], true);
    fs.delete(databusFiles[4], true);
    fs.delete(databusFiles[5], true);
    databusFiles[6] = TestUtil.copyFileToStreamLocal(fs, testStream,
        collectorName, cluster, collectorDir, files[6]);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[4]), 5, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[5]), 6, 0, 50, partitionId, buffer, true);
    Assert.assertEquals(((CollectorReader)preader.getReader()).
        getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    fs.delete(databusFiles[6], true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[5]), 6, 50, 50, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[6]), 7, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[7]),
        8, 0, 100, partitionId, buffer, true);
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertTrue(buffer.isEmpty());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 700);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 700);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }
}
