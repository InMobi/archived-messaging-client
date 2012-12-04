package com.inmobi.databus.readers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestLocalStreamReader extends TestAbstractDatabusWaitingReader{
  protected Cluster cluster;

  @BeforeTest
  public void setup() throws Exception {
  	consumerNumber = 1;
    // initialize config
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
    testStream, new PartitionId(clusterName, collectorName), files, null,
    finalFiles, 3, 0);
    conf = cluster.getHadoopConf();
    fs = FileSystem.get(conf);
    streamDir = getStreamsDir();
    inputFormatClass = TextInputFormat.class.getCanonicalName();
    encoded = true;
    partitionMinList = new ArrayList<Integer>();
    for (int i = 0; i < 60; i++) {
    	partitionMinList.add(i);
    }
    chkPoints = new TreeMap<Integer, PartitionCheckpoint>();
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
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
  public void testReadFromTimeStamp() throws Exception {
    super.testReadFromTimeStamp();
  }

  @Override
  Path getStreamsDir() {
    return DatabusStreamReader.getStreamsLocalDir(cluster, testStream);
  }

}
