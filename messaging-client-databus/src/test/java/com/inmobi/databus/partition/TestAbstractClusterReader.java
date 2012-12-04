package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;
import org.testng.Assert;

import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public abstract class TestAbstractClusterReader {
  protected static final String testStream = "testclient";
  protected static final String clusterName = "testCluster";
  protected PartitionId partitionId = new PartitionId(clusterName, null);
  protected LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  protected PartitionReader preader;
  public List<Integer> partitionMinList;                                             
  PartitionCheckpointList partitionCheckpointList;                                                      
  Map<Integer, PartitionCheckpoint> pchkPoints;                                        
  int consumerNumber;

  protected String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  protected Path[] databusFiles = new Path[3];

  protected final String collectorName = "collector1";
  FileSystem fs;
  Path streamDir;
  Configuration conf = new Configuration();
  String inputFormatClass;
  DataEncodingType dataEncoding;

  public void cleanup() throws IOException {
    fs.delete(streamDir.getParent(), true);
  }

  abstract Path getStreamsDir();
  abstract boolean isDatabusData();

  public void testInitialize() throws Exception {
  	initializeMinList();
    Map<Integer, PartitionCheckpoint> chkpoints = new TreeMap<Integer, 
  			PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints);
    
  	PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
    // Read from start
    preader = new PartitionReader(partitionId, null, fs, buffer,
        streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), dataEncoding, prMetrics, partitionMinList);              
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].toString());

    // Read from checkpoint with local stream file name
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1], 
        partitionCheckpointList);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
        streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), dataEncoding, prMetrics, partitionMinList); 
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].toString());

    // Read from checkpoint with local stream file name which does not exist
    // and is before the stream
    prepareCheckpoint(HadoopUtil.getOlderFile(streamDir, fs, databusFiles[0]), 
    		20, databusFiles[1], partitionCheckpointList);
  
    preader = new PartitionReader(partitionId, partitionCheckpointList,
        fs, buffer, streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), dataEncoding, prMetrics, partitionMinList); 
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].toString());

    //Read from startTime in local stream directory, with no checkpoint
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[1].getParent()));

    preader = new PartitionReader(partitionId, null, fs, buffer,
        streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), dataEncoding, prMetrics, partitionMinList);              
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].toString());

    //Read from startTime in local stream directory, with checkpoint
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1], 
        partitionCheckpointList);
    preader = new PartitionReader(partitionId, partitionCheckpointList,fs, buffer,
            streamDir, conf, inputFormatClass, cal.getTime(), 1000,
            isDatabusData(), dataEncoding, prMetrics, partitionMinList);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].toString());

    //Read from startTime in local stream directory, with no timestamp file,
    // with no checkpoint
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
    cal.add(Calendar.MINUTE, 1);
    preader = new PartitionReader(partitionId, null, fs, buffer,
        streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), dataEncoding, prMetrics, partitionMinList);              
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].toString());

    //Read from startTime in local stream directory, with no timestamp file,
    //with checkpoint
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1], 
        partitionCheckpointList);
    preader = new PartitionReader(partitionId,partitionCheckpointList, fs, buffer,
            streamDir, conf, inputFormatClass, cal.getTime(), 1000,
            isDatabusData(), dataEncoding, prMetrics, partitionMinList);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].toString());

    //Read from startTime beyond the stream
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
    cal.add(Calendar.MINUTE, -2);
    preader = new PartitionReader(partitionId, null, fs, buffer,
        streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), dataEncoding, prMetrics, partitionMinList);              
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].toString());

    //Read from startTime beyond the stream, with checkpoint
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1], 
        partitionCheckpointList);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
            streamDir, conf, inputFormatClass, cal.getTime(), 1000,
            isDatabusData(), dataEncoding, prMetrics, partitionMinList);     
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].toString());

    //Read from startTime after the stream
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[2].getParent()));
    cal.add(Calendar.MINUTE, 2);
    preader = new PartitionReader(partitionId,
        null, fs, buffer,
        streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), dataEncoding, prMetrics, true, partitionMinList);       
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());

    //Read from startTime after the stream, with checkpoint
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1], 
        partitionCheckpointList);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
            streamDir, conf, inputFormatClass, cal.getTime(), 1000,
            isDatabusData(), dataEncoding, prMetrics, true, partitionMinList); 
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());
  }

  public void testReadFromStart() throws Exception {
  	initializeMinList();
    Map<Integer, PartitionCheckpoint> chkpoints = new TreeMap<Integer, 
  			PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints); 
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, 
    		buffer, streamDir, conf, inputFormatClass,
        DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
                databusFiles[0].getParent()),
        1000, isDatabusData(), dataEncoding, prMetrics, true, partitionMinList);        
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromCheckpoint() throws Exception {
  	initializeMinList();
    Map<Integer, PartitionCheckpoint> chkpoints = new TreeMap<Integer, 
  			PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints);
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1], 
        partitionCheckpointList);
   
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
            streamDir, conf, inputFormatClass, null, 1000,
            isDatabusData(), dataEncoding, prMetrics, true, partitionMinList);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 20, 80, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 180);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 180);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
  	initializeMinList();
    Map<Integer, PartitionCheckpoint> chkpoints = new TreeMap<Integer, 
  			PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints);
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    prepareCheckpoint(HadoopUtil.getOlderFile(streamDir, fs, databusFiles[0]), 
    		20, databusFiles[1], partitionCheckpointList);

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
        streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), dataEncoding, prMetrics, true, partitionMinList); 
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromStartTime() throws Exception {
  	initializeMinList();
    Map<Integer, PartitionCheckpoint> chkpoints = new TreeMap<Integer, 
  			PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints);
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1], 
        partitionCheckpointList);

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, 
    				buffer, streamDir, conf, inputFormatClass,
            DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
                databusFiles[1].getParent()),
            1000,
            isDatabusData(), dataEncoding, prMetrics, true, partitionMinList);   
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 200);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromStartTimeWithinStream() throws Exception {
  	initializeMinList();
    Map<Integer, PartitionCheckpoint> chkpoints = new TreeMap<Integer, 
  			PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
    cal.add(Calendar.MINUTE, 1);
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1], 
        partitionCheckpointList);

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
            streamDir, conf, inputFormatClass, cal.getTime(), 1000,
            isDatabusData(), dataEncoding, prMetrics, true, partitionMinList); 
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 200);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0); 
  }

  public void testReadFromStartTimeBeforeStream() throws Exception {
  	initializeMinList();
    Map<Integer, PartitionCheckpoint> chkpoints = new TreeMap<Integer, 
  			PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints); 
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
    cal.add(Calendar.MINUTE, -1);
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1], 
        partitionCheckpointList);

    preader = new PartitionReader(partitionId, partitionCheckpointList,  fs, buffer,
            streamDir, conf, inputFormatClass, cal.getTime(), 1000,
            isDatabusData(), dataEncoding, prMetrics, true, partitionMinList);  
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 20, 80, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 280);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 280);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0); 
  }

  public void testReadFromStartTimeAfterStream() throws Exception {
  	initializeMinList();
    Map<Integer, PartitionCheckpoint> chkpoints = new TreeMap<Integer, 
  			PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints); 
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[2].getParent()));
    cal.add(Calendar.MINUTE, 2);
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1], 
        partitionCheckpointList);
    preader = new PartitionReader(partitionId, partitionCheckpointList,  fs, buffer,
            streamDir, conf, inputFormatClass, cal.getTime(), 1000,
            isDatabusData(), dataEncoding, prMetrics, true, partitionMinList);        
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 0);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 0);
    Assert.assertEquals(prMetrics.getCumulativeNanosForFetchMessage(), 0);
  }
  
  public void testReadFromCheckpointWithSingleMinute() throws Exception {
  	partitionMinList = new ArrayList<Integer>();
  	Map<Integer, PartitionCheckpoint> chkpoints = new 
  			TreeMap<Integer, PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints);
  	
  	for (int i =0; i < 1; i++) {
  		Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir, 
  				databusFiles[i].getParent()) ; 
  		Log.info("date is " + date);
  		partitionMinList.add(date.getMinutes());
  		partitionCheckpointList.set(date.getMinutes(), new PartitionCheckpoint(
  				DatabusStreamWaitingReader.getHadoopStreamFile(fs.getFileStatus(
  						databusFiles[i])), 20)); 
  		Log.info("minute is" + partitionMinList.get(i));
  	}
  	
  	PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
   
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
        streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), dataEncoding, prMetrics, true, partitionMinList);             
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].toString());
  	
    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 20, 80, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 80);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 80);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);  
  }
  
  public void testReadFromCheckpointMultipleMinutes() throws Exception {
  	partitionMinList = new ArrayList<Integer>();
  	Map<Integer, PartitionCheckpoint> chkpoints = new 
  			TreeMap<Integer, PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints);
  	
  	for (int i =0; i < 3; i++) {
  		Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir, 
  				databusFiles[i].getParent()) ; 
  		Log.info("date is " + date);
  		partitionMinList.add(date.getMinutes());
  		partitionCheckpointList.set(date.getMinutes(), new PartitionCheckpoint(
  				DatabusStreamWaitingReader.getHadoopStreamFile(fs.getFileStatus(
  						databusFiles[i])), 20)); 
  		Log.info("minute is" + partitionMinList.get(i));
  	}
  	
  	PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
 
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
        streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), dataEncoding, prMetrics,true, partitionMinList);             
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].toString());
  	
    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 20, 80, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 20, 80, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 20, 80, partitionId,
        buffer, dataEncoding.equals(DataEncodingType.BASE64)); 
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 240);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 240);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0); 
  }
  
  public void testReadFromCheckpointSomeMinutes()  throws Exception {
  	partitionMinList = new ArrayList<Integer>();
  	Map<Integer, PartitionCheckpoint> chkpoints = new 
  			TreeMap<Integer, PartitionCheckpoint>();
  	partitionCheckpointList = new PartitionCheckpointList(chkpoints);
  	for (int i =0; i < 3; i++) {
  		Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir, 
  				databusFiles[i].getParent()) ; 
  		Log.info("date is " + date);
  		partitionMinList.add(date.getMinutes());
  		if (i != 1) {
  			partitionCheckpointList.set(date.getMinutes(), new PartitionCheckpoint(
  					DatabusStreamWaitingReader.getHadoopStreamFile(fs.getFileStatus(
  							databusFiles[i])), 20)); 
  		} else {
  			partitionCheckpointList.set(date.getMinutes(), new PartitionCheckpoint(
  					DatabusStreamWaitingReader.getHadoopStreamFile(fs.getFileStatus(
  							databusFiles[i])), 00)); 
  		}
  		Log.info("minute is" + partitionMinList.get(i));
  	}

  	PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
  			testStream, "c1", partitionId.toString(), consumerNumber);
  	Calendar cal = Calendar.getInstance();
  	cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
  			databusFiles[0].getParent()));

  	preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
  			streamDir, conf, inputFormatClass, null, 1000,
  			isDatabusData(), dataEncoding, prMetrics, true, partitionMinList);             
  	preader.init();
  	Assert.assertEquals(preader.getCurrentFile().toString(),
  			databusFiles[0].toString());

  	preader.execute();
  	TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
  			fs.getFileStatus(databusFiles[1])), 1, 20, 80, partitionId,
  			buffer, dataEncoding.equals(DataEncodingType.BASE64));
  	TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
  			fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
  			buffer, dataEncoding.equals(DataEncodingType.BASE64));
  	TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
  			fs.getFileStatus(databusFiles[2])), 3, 20, 80, partitionId,
  			buffer, dataEncoding.equals(DataEncodingType.BASE64));
  	Assert.assertTrue(buffer.isEmpty());
  	Assert.assertNotNull(preader.getReader());
  	Assert.assertEquals(preader.getReader().getClass().getName(),
  			ClusterReader.class.getName());
  	Assert.assertEquals(((ClusterReader)preader.getReader())
  			.getReader().getClass().getName(),
  			DatabusStreamWaitingReader.class.getName());
  	Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
  	Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 260);
  	Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 260);
  	Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
  	Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0); 
  }
  
  public void prepareCheckpoint(StreamFile streamFile, int lineNum, 
  		Path databusFile, PartitionCheckpointList partitionCheckpointList) {
  	
  	Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir, 
				databusFile.getParent());
  	partitionCheckpointList.set(date.getMinutes(), new PartitionCheckpoint(
  			streamFile, lineNum));
  }
  
  public void initializeMinList() {
  	partitionMinList = new ArrayList<Integer>();
    for (int i =0; i < 60; i++) {
    	partitionMinList.add(i);
    }
  }
}
