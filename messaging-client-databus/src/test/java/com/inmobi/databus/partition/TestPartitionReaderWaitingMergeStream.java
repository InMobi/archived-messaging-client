package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderWaitingMergeStream
    extends TestAbstractWaitingClusterReader {

  @BeforeTest
  public void setup() throws Exception {
    files = new String[] {TestUtil.files[1],
        TestUtil.files[3], TestUtil.files[5]};
    newFiles = new String[] {TestUtil.files[6],
        TestUtil.files[7], TestUtil.files[8] };
    inputFormatClass = TextInputFormat.class.getName();
    dataEncoding = DataEncodingType.BASE64;
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectorName), files, null,
        databusFiles, 0, 3);
    conf = cluster.getHadoopConf();
    fs = FileSystem.get(conf);
    streamDir = DatabusUtil.getStreamDir(StreamType.MERGED,
        new Path(cluster.getRootDir()), testStream);
    partitionMinList = new ArrayList<Integer>();
    for (int i =0; i< 60; i++) {
    	partitionMinList.add(i);
    }
    Map<Integer, PartitionCheckpoint> list = new TreeMap<Integer, PartitionCheckpoint>();
    partitionCheckpointlist = new PartitionCheckpointList(list);
  }

  void setupFiles(String[] files, Path[] newDatabusFiles) throws Exception {
    TestUtil.setUpFiles(cluster, collectorName, files, null, newDatabusFiles,
        0, files.length);
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }

  @Test
  public void testReadFromStart() throws Exception {
    super.testReadFromStart();
  }

  @Override
  boolean isDatabusData() {
    return true;
  }
}
