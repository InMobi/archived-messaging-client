package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.consumer.util.MiniClusterUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class TestClusterReaderMultipleCollectors {

  private static final String testStream = "testclient";

  private String[] collectors = new String[] {"collector1", "collector2"};
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, null);
  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(149);
  private PartitionReader preader;
  private Cluster cluster;
  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5], TestUtil.files[6]};
  Path[] databusFiles1 = new Path[3];
  Path[] databusFiles2 = new Path[3];
  FileSystem fs;
  Path streamDir;
  Configuration conf = new Configuration();
  Set<Integer> partitionMinList;                                                  
  PartitionCheckpointList partitionCheckpointList;    
  int consumerNumber;

  @BeforeTest
  public void setup() throws Exception {
    // initialize config
  	consumerNumber = 1;
    fs = MiniClusterUtil.getDFSCluster(conf).getFileSystem();
    cluster = TestUtil.setupDFSCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectors[0]),
        fs.getUri().toString(), files, null,
        databusFiles1, 0, 1);
    TestUtil.setUpFiles(cluster, collectors[1], files, null, databusFiles2, 0,
        1);
    streamDir = DatabusUtil.getStreamDir(StreamType.MERGED,
        new Path(cluster.getRootDir()), testStream);
    Map<Integer, PartitionCheckpoint> chkpoints = new 
    		TreeMap<Integer, PartitionCheckpoint>();
    partitionCheckpointList = new PartitionCheckpointList(chkpoints);
    partitionMinList = new TreeSet<Integer>();
    for (int i = 0; i < 60; i++) {
    	partitionMinList.add(i);
    }
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
    MiniClusterUtil.shutdownDFSCluster();
  }

  @Test
  public void testReadFromStart() throws Exception {
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, 
    		buffer, streamDir, conf, TextInputFormat.class.getCanonicalName(),
        CollectorStreamReader.getDateFromCollectorFile(files[0]), 1000, true,
        DataEncodingType.BASE64, prMetrics, false, partitionMinList);            
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.start();
    // move file11 
    TestUtil.incrementCommitTime();
    Path movedPath1 = TestUtil.moveFileToStreams(fs, testStream, collectors[1],
        cluster, TestUtil.getCollectorDir(cluster, testStream, collectors[1]),
        files[1]);
    
    // read file00, file10
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles1[0])), 1, 0, 100, partitionId,
        buffer, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles2[0])), 1, 0, 50, partitionId,
        buffer, true);
    
    // move file01, file12
    TestUtil.incrementCommitTime();
    Path movedPath2 = TestUtil.moveFileToStreams(fs, testStream, collectors[0],
        cluster, TestUtil.getCollectorDir(cluster, testStream, collectors[0]),
        files[1]);
    Path movedPath3 = TestUtil.moveFileToStreams(fs, testStream, collectors[1],
        cluster, TestUtil.getCollectorDir(cluster, testStream, collectors[1]),
        files[2]);
    
    // read file10, file11
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles2[0])), 1, 50, 50, partitionId,
        buffer, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath1)), 2, 0, 100, partitionId,
        buffer, true);
    
    // move file02
    TestUtil.incrementCommitTime();
    Path movedPath4 = TestUtil.moveFileToStreams(fs, testStream, collectors[0],
        cluster, TestUtil.getCollectorDir(cluster, testStream, collectors[0]),
        files[2]);
    // read file10, file12
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath2)), 2, 0, 100, partitionId,
        buffer, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath3)), 3, 0, 50, partitionId,
        buffer, true);
    
    // move file13
    TestUtil.incrementCommitTime();
    Path movedPath5 = TestUtil.moveFileToStreams(fs, testStream, collectors[1],
    cluster, TestUtil.getCollectorDir(cluster, testStream, collectors[1]),
    files[3]);
    
    //read file12, file02
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath3)), 3, 50, 50, partitionId,
    buffer, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath4)), 3, 0, 50, partitionId,
    buffer, true);
    
    //move file03
    Path movedPath6 = TestUtil.moveFileToStreams(fs, testStream, collectors[0],
    cluster, TestUtil.getCollectorDir(cluster, testStream, collectors[0]),
    files[3]);
    TestUtil.publishLastPathForStreamsDir(fs, cluster, testStream);
    // read file02, file13, file03
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath4)), 3, 50, 50, partitionId,
        buffer, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath5)), 4, 0, 100, partitionId,
    buffer, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath6)), 4, 0, 100, partitionId,
    buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    preader.close();
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 800);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 800);
    Assert.assertTrue(prMetrics.getWaitTimeUnitsNewFile() > 0);

    prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    prepareCheckpoint( DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath5)), 50, movedPath5, partitionCheckpointList);
    preader = new PartitionReader(partitionId,  partitionCheckpointList, fs, 
    		buffer, streamDir, conf, TextInputFormat.class.getCanonicalName(),
    		null, 1000, true, DataEncodingType.BASE64, prMetrics, false, 
    		partitionMinList); 
    preader.start();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath5)), 4, 50, 50, partitionId,
    buffer, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(movedPath6)), 4, 0, 100, partitionId,
    buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    preader.close();
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 150);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 150);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }
  
  public void prepareCheckpoint(StreamFile streamFile, int lineNum, 
  		Path databusFile, PartitionCheckpointList partitionCheckpointList) {
  	Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir, 
				databusFile.getParent());
  	partitionCheckpointList.set(date.getMinutes(), new PartitionCheckpoint(
  			streamFile, lineNum));
  }
}
