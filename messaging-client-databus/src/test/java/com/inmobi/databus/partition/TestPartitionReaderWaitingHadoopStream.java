package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.util.HadoopUtil;

public class TestPartitionReaderWaitingHadoopStream extends 
    TestAbstractWaitingClusterReader {

  @BeforeTest
  public void setup() throws Exception {
  	consumerNumber = 1;
    conf = new Configuration();
    files = new String[] {HadoopUtil.files[1],
        HadoopUtil.files[3], HadoopUtil.files[5]};
    newFiles = new String[] {HadoopUtil.files[6],
        HadoopUtil.files[7], HadoopUtil.files[8] };
    // setup fs
    fs = FileSystem.getLocal(conf);
    streamDir = new Path("/tmp/test/hadoop/" + this.getClass().getSimpleName(),
         testStream).makeQualified(fs);
    HadoopUtil.setupHadoopCluster(conf, files, null, databusFiles, streamDir);
    inputFormatClass = SequenceFileInputFormat.class.getName();
    dataEncoding = DataEncodingType.NONE;
    partitionMinList = new ArrayList<Integer>();
    for (int i =0; i< 60; i++) {
    	partitionMinList.add(i);
    }
    Map<Integer, PartitionCheckpoint> list = new 
    		TreeMap<Integer, PartitionCheckpoint>();
    partitionCheckpointlist = new PartitionCheckpointList(list);
  }

  @AfterTest
  public void cleanup() throws IOException {
    fs.delete(streamDir.getParent(), true);
  }

  @Override
  void setupFiles(String[] files, Path[] newDatabusFiles) throws Exception {
    HadoopUtil.setUpHadoopFiles(streamDir, conf, files, null, newDatabusFiles);    
  }

  @Test
  public void testReadFromStart() throws Exception {
    super.testReadFromStart();
  }

  @Override
  boolean isDatabusData() {
    return false;
  }
}
