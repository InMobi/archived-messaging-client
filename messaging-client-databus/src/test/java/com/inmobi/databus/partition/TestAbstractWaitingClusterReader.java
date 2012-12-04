package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public abstract class TestAbstractWaitingClusterReader {

  protected static final String testStream = "testclient";
  protected static final String clusterName = "testCluster";
  protected PartitionId partitionId = new PartitionId(clusterName, null);

  protected LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(150);
  protected Cluster cluster;
  protected PartitionReader preader;
  List<Integer>  partitionMinList;                                                     
  PartitionCheckpointList partitionCheckpointlist;                                                      

  
  protected String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  protected String[] newFiles = new String[] {TestUtil.files[6], TestUtil.files[7],
      TestUtil.files[8]};
  protected Path[] databusFiles = new Path[3];

  protected final String collectorName = "collector1";
  FileSystem fs;
  String inputFormatClass;
  DataEncodingType dataEncoding;
  Path streamDir;
  Configuration conf;
  int consumerNumber;

  abstract void setupFiles(String[] files, Path[] newDatabusFiles) throws
      Exception;
  abstract boolean isDatabusData();
  
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  public void testReadFromStart() throws Exception {
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, partitionCheckpointlist, fs, 
    		buffer, streamDir, conf, inputFormatClass,
        DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
            databusFiles[0]),
        1000, isDatabusData(), dataEncoding, prMetrics, false, partitionMinList);     

    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.start();
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    FileStatus fs0 = fs.getFileStatus(databusFiles[0]); 
    FileStatus fs1 = fs.getFileStatus(databusFiles[1]); 
    fs.delete(databusFiles[0], true);
    fs.delete(databusFiles[1], true);
    fs.delete(databusFiles[2], true);
    Path[] newDatabusFiles = new Path[3];
    setupFiles(new String[] {newFiles[0]}, newDatabusFiles);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs0), 1, 0, 100, partitionId, buffer,
        dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs1), 2, 0, 50, partitionId, buffer,
        dataEncoding.equals(DataEncodingType.BASE64));
    
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs1), 2, 50, 50, partitionId, buffer, dataEncoding.equals(
            DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(newDatabusFiles[0])), 1, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    setupFiles(new String[] {newFiles[1], newFiles[2]},
        newDatabusFiles);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(newDatabusFiles[0])), 1, 0, 100, partitionId, buffer,
        dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(newDatabusFiles[1])), 2, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    Assert.assertTrue(buffer.isEmpty());    
    preader.close();
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 500);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 500);
    Assert.assertTrue(prMetrics.getWaitTimeUnitsNewFile() > 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }
}
