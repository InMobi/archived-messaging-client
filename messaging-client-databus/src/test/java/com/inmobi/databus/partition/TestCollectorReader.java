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
import com.inmobi.databus.partition.PartitionCheckpoint;
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

public class TestCollectorReader {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  private Cluster cluster;
  private PartitionReader preader;
  private String[] files = new String[] {TestUtil.files[1],
      TestUtil.files[2], TestUtil.files[4], TestUtil.files[6],
      TestUtil.files[8], TestUtil.files[10]};
  private String[] emptyfiles = new String[] {TestUtil.files[5],
      TestUtil.files[9]};
  private Path[] databusFiles = new Path[3];
  private String doesNotExist1 = TestUtil.files[0];
  private String doesNotExist2 = TestUtil.files[3];
  private String doesNotExist3 = TestUtil.files[7];
  private String doesNotExist4 = TestUtil.files[11];
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
        testStream, partitionId, files, emptyfiles, databusFiles, 3);
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
  public void testInitialize() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    // Read from no where
    Throwable th = null;
    try {
      preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream, null, 1000, 1000,
        DataEncodingType.BASE64, prMetrics);
      preader.init();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    // Read from starttime of stream
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir,
        buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[0]), 1000, 1000,
        DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    // Read from checkpoint with collector file name, but file in local stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream, null,
        1000, 1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    // Read from checkpoint with local stream file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(
            LocalStreamCollectorReader.getDatabusStreamFile(
            collectorName, files[1]), 20),
            conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
            1000, 1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    // Read from checkpoint with collector file name and file in collector
    // stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[4]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream, null,
        1000, 1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[4]);    

    // Read from checkpoint with collector file name which does not exist
    // and is before the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 40), conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream,
        null, 1000, 1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    // Read from checkpoint with local stream file name which does not exist
    // and is before the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist1), 20),
            conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
            1000, 1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    // Read from checkpoint with collector file name which does not exist
    // but the collector file time stamp is after the stream
    th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
          CollectorStreamReader.getCollectorFile(doesNotExist4), 40), conf,
          fs, collectorDir, streamsLocalDir, buffer,
          testStream, null, 1000, 1000, DataEncodingType.BASE64, prMetrics, true);
      preader.init();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    // Read from checkpoint with local stream file name which does not exist
    // but the file time stamp is after the stream
    th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
          LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
              doesNotExist4), 20),
              conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
              1000, 1000, DataEncodingType.BASE64, prMetrics, true);
      preader.init();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
    
    // Read from checkpoint with collector file name which does not exist
    // but the collector file time stamp is within the stream
    th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
          CollectorStreamReader.getCollectorFile(doesNotExist3), 40), conf,
          fs, collectorDir, streamsLocalDir, buffer, testStream,
          null, 10, 1000, DataEncodingType.BASE64, prMetrics, true);
      preader.init();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    //Read from startTime in local stream directory, with no checkpoint
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    //Read from startTime in local stream directory, with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), conf, fs,
        collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000,
        1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    //Read from startTime in local stream directory, with no timestamp file,
    // with no checkpoint
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[2].getName());

    //Read from startTime in local stream directory, with no timestamp file,
    //with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[2].getName());

    //Read from startTime in collector dir, with no checkpoint
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[4]), 1000, 1000,
        DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[4]);    

    //Read from startTime in collector dir, with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[4]), 1000, 1000,
        DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[4]);

    //Read from startTime in collector dir, with no timestamp file,
    // with no checkpoint
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer, testStream, 
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[4]);    

    //Read from startTime in collector dir, with no timestamp file,
    //with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), conf, fs,
        collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[4]);

    //Read from startTime beyond the stream
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    //Read from startTime after the stream
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist4), 1000,
        1000, DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());

    //Read from startTime beyond the stream, with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, DataEncodingType.BASE64, prMetrics);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    //Read from startTime after the stream, with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist4), 1000,
        1000, DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());
  }

  @Test
  public void testReadFromStart() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[0]), 10, 1000,
        DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[3]), 4,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 600);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 600);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromCheckpointWithCollectorFileName() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream, null,
        10, 1000, DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  20, 80, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[3]), 4,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 480);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 480);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromCheckpointWithLocalStreamFileName() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            files[1]), 20),
        conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
        10, 1000, DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  20, 80, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[3]), 4,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 480);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 480);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromCheckpointWithCollectorFile() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[4]), 40), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream, null,
        10, 1000, DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5,
        40, 60, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 160);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 160);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  /**
   *  Disable this test if partition reader should not read from start of stream
   *  if check point does not exist.
   */
  
  @Test
  public void testReadFromCheckpointWithCollectorFileWhichDoesNotExist()
      throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 40), conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream, null,
        10, 1000, DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[3]), 4,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 600);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 600);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  /**
   *  Disable this test if partition reader should not read from start of stream
   *  if check point does not exist.
   */
  @Test
  public void testReadFromCheckpointWithLocalStreamFileWhichDoesNotExist()
      throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist1), 20),
            conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
            10, 1000, DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[3]), 4,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 600);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 600);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromStartTimeInLocalStream() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 10, 1000,
        DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[3]), 4,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 500);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 500);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromStartTimeInLocalStream2() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 10, 1000,
        DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[3]), 4,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 400);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 400);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromStartTimeInCollectorStream() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[4]), 10, 1000,
        DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5
        ,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 200);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromStartTimeInCollectorStream2() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 10, 1000,
        DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 200);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 10, 1000,
        DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[3]), 4,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]), 5,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]), 6,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 600);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 600);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist4), 10, 1000,
        DataEncodingType.BASE64, prMetrics, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 0);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertEquals(prMetrics.getCumulativeNanosForFetchMessage(), 0);
  } 
}
