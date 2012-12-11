package com.inmobi.databus.readers;

import java.io.IOException;
import java.util.Calendar;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.testng.Assert;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public abstract class TestAbstractDatabusWaitingReader {
  protected static final String testStream = "testclient";

  protected static final String collectorName = "collector1";
  protected static final String clusterName = "testCluster";
  protected PartitionId partitionId = new PartitionId(clusterName, null);
  protected DatabusStreamWaitingReader lreader;
  protected String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  protected Path[] finalFiles = new Path[3];
  protected FileSystem fs;
  protected Configuration conf;
  protected Path streamDir;
  protected String inputFormatClass;
  protected boolean encoded;
  public Set<Integer> partitionMinList;                                              
  public PartitionCheckpointList partitionCheckpointList;  
  Map<Integer, PartitionCheckpoint> chkPoints;
  int consumerNumber;

  public void cleanup() throws IOException {
  }

  abstract Path getStreamsDir();

  public void testInitialize() throws Exception {
    PartitionReaderStatsExposer metrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    // Read from start
    lreader = new DatabusStreamWaitingReader(partitionId,
        fs, streamDir,
        inputFormatClass, conf, 1000, metrics, false, partitionMinList, 
        partitionCheckpointList);           
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[0].getParent()));
    lreader.build(cal.getTime());

    lreader.initFromStart();
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[0]);

    // Read from checkpoint with stream file name
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        DatabusStreamWaitingReader.getHadoopStreamFile(
            fs.getFileStatus(finalFiles[1])), 20));
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[1]);

    // Read from checkpoint with stream file name, which does not exist
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        HadoopUtil.getOlderFile(streamDir, fs, finalFiles[0]), 20));
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[0]);

    //Read from startTime in stream directory, before the stream
    cal.add(Calendar.MINUTE, -2);
    lreader.initializeCurrentFile(cal.getTime());
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[0]);

    //Read from startTime within stream directory
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[1].getParent()));
    lreader.initializeCurrentFile(cal.getTime());
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[1]);

    //Read from startTime within the stream, but no min directory
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[0].getParent()));
    cal.add(Calendar.MINUTE, 1);
    lreader.initializeCurrentFile(cal.getTime());
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[1]);

    //Read from startTime in after the stream
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[2].getParent()));
    cal.add(Calendar.MINUTE, 2);
    lreader.initializeCurrentFile(cal.getTime());
    Assert.assertNull(lreader.getCurrentFile());  

    // startFromNextHigher with filename
    lreader.startFromNextHigher(fs.getFileStatus(finalFiles[1]));
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[2]);

    // startFromTimestamp with date
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[1].getParent()));
    lreader.startFromTimestmp(cal.getTime());
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[1]);
    
    // startFromBegining 
   lreader.startFromBegining();
   Assert.assertEquals(lreader.getCurrentFile(), finalFiles[0]);
  }

  static void readFile(StreamReader reader, int fileNum,
      int startIndex, Path filePath, boolean encoded)
      throws Exception {
    int fileIndex = fileNum * 100 ;
    for (int i = startIndex; i < 100; i++) {
      byte[] line = reader.readLine();
      Text text = MessageUtil.getTextMessage(line);
      Assert.assertNotNull(line);
      if (encoded) {
        Assert.assertEquals(new String(Base64.decodeBase64(text.getBytes())),
            MessageUtil.constructMessage(fileIndex + i));
      } else {
        Assert.assertEquals(text,
            new Text(MessageUtil.constructMessage(fileIndex + i)));        
      }
    }
    Assert.assertEquals(reader.getCurrentFile(), filePath);
  }


  public void testReadFromStart() throws Exception {
  	initializePartitionCheckpointList();
    PartitionReaderStatsExposer metrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    lreader = new DatabusStreamWaitingReader(partitionId,
        fs, getStreamsDir(),
        inputFormatClass, conf , 1000, metrics, false, partitionMinList, 
        partitionCheckpointList);              
    lreader.build(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[0].getParent()));
    lreader.initFromStart();
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(lreader, 0, 0, finalFiles[0], encoded);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 100);
    readFile(lreader, 1, 0, finalFiles[1], encoded);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 200);
    readFile(lreader, 2, 0, finalFiles[2], encoded);
    lreader.close();
    Assert.assertEquals(metrics.getHandledExceptions(), 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(metrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(metrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromCheckpoint() throws Exception {
  	initializePartitionCheckpointList();
    PartitionReaderStatsExposer metrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    lreader = new DatabusStreamWaitingReader(partitionId,
        fs, getStreamsDir(), inputFormatClass, conf, 1000, metrics, false, 
        partitionMinList, partitionCheckpointList);  
    PartitionCheckpoint pcp = new PartitionCheckpoint(
        DatabusStreamWaitingReader.getHadoopStreamFile(
            fs.getFileStatus( finalFiles[1])), 20);
    lreader.build(DatabusStreamWaitingReader.getBuildTimestamp(streamDir, pcp));
    lreader.initializeCurrentFile(pcp);
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(lreader, 1, 20, finalFiles[1], encoded);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 80);
    readFile(lreader, 2, 0, finalFiles[2], encoded);
    lreader.close();
    Assert.assertEquals(metrics.getHandledExceptions(), 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 180);
    Assert.assertEquals(metrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(metrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromTimeStamp() throws Exception {
  	initializePartitionCheckpointList();
  	PartitionReaderStatsExposer metrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    lreader = new DatabusStreamWaitingReader(partitionId,
        fs, getStreamsDir(), inputFormatClass, conf, 1000, metrics, false, 
        partitionMinList, partitionCheckpointList);  
    lreader.build(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[1].getParent()));
    lreader.initializeCurrentFile(
        DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[1].getParent()));
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(lreader, 1, 0, finalFiles[1], encoded);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 100);
    readFile(lreader, 2, 0, finalFiles[2], encoded);
    lreader.close();
    Assert.assertEquals(metrics.getHandledExceptions(), 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(metrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(metrics.getCumulativeNanosForFetchMessage() > 0);
  }
  public void initializePartitionCheckpointList() {
  	chkPoints = new TreeMap<Integer, PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkPoints);
  }
}
