package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;

public class TestCurrentFile {
  private static final String testStream = "testclient";
  private static final String collectorName = "collector1";
  private static final String clusterName = "testDFSCluster";

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);

  private FileSystem fs;
  private Cluster cluster;
  private Path collectorDir;
  private int msgIndex = 300;
  private PartitionReader preader;
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private String currentScribeFile = TestUtil.files[3];
  MiniDFSCluster dfsCluster;
  Configuration conf = new Configuration();

  private void writeMessages(FSDataOutputStream out, int num)
      throws IOException {
    for (int i = 0; i < num; i++) {
      out.write(Base64.encodeBase64(
          TestUtil.constructMessage(msgIndex).getBytes()));
      out.write('\n');
      msgIndex++;
    }  
    out.sync();
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @BeforeTest
  public void setup() throws Exception {     
    dfsCluster = new MiniDFSCluster(conf, 1, true,null);
    cluster = TestUtil.setupDFSCluster(this.getClass().getSimpleName(),
        testStream,
        partitionId, dfsCluster.getFileSystem().getUri().toString(),
        null, null, 0);
    collectorDir = new Path(new Path(cluster.getDataDir(), testStream),
        collectorName);
    fs = FileSystem.get(cluster.getHadoopConf());
  }

  @Test
  public void testReadFromCurrentScribeFile() throws Exception {
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(currentScribeFile),
        1000);
    preader.start();
    FSDataOutputStream out = fs.create(
        new Path(collectorDir, currentScribeFile));
    writeMessages(out, 10);
    Assert.assertTrue(buffer.isEmpty());
    TestUtil.assertBuffer(currentScribeFile, 4, 0, 10, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    writeMessages(out, 20);
    TestUtil.assertBuffer(currentScribeFile, 4, 10, 20, partitionId, buffer);
    writeMessages(out, 20);
    TestUtil.assertBuffer(currentScribeFile, 4, 30, 20, partitionId, buffer);
    writeMessages(out, 50);
    out.close();
    TestUtil.setUpEmptyFiles(fs, collectorDir, TestUtil.files[5]);
    TestUtil.assertBuffer(currentScribeFile, 4, 50, 50, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
  }

}
