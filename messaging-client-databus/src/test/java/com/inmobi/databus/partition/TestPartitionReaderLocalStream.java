package com.inmobi.databus.partition;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderLocalStream extends TestAbstractClusterReader {
  Cluster cluster;
  @BeforeTest
  public void setup() throws Exception {
  	consumerNumber = 1;
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectorName), files, null,
        databusFiles, 3);
    fs = FileSystem.get(cluster.getHadoopConf());
    streamDir = DatabusStreamWaitingReader.getStreamsLocalDir(cluster,
        testStream);
    inputFormatClass = TextInputFormat.class.getName();
    dataEncoding = DataEncodingType.BASE64;
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
    fs.delete(new Path(cluster.getRootDir()), true);
  }

  @Test
  public void testInitialize() throws Exception {
    super.testInitialize();
  }

  @Test
  public void testReadFromStart() throws Exception {
    super.testReadFromStart();
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    super.testReadFromCheckpoint();
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    super.testReadFromCheckpointWhichDoesNotExist();
  }

  @Test
  public void testReadFromStartTime() throws Exception {
    super.testReadFromStartTime();
  }

  @Test
  public void testReadFromStartTimeWithinStream() throws Exception {
    super.testReadFromStartTimeWithinStream();
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    super.testReadFromStartTimeBeforeStream();
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    super.testReadFromStartTimeAfterStream();
  }

  @Override
  Path getStreamsDir() {
    return streamDir;
  }

  @Override
  boolean isDatabusData() {
    return true;
  }

}
