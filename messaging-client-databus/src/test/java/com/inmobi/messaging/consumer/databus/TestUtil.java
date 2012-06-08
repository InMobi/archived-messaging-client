package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.utils.FileUtil;

public class TestUtil {
  private static final Log LOG = LogFactory.getLog(TestUtil.class);

  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  static final PartitionId partitionId = new PartitionId(clusterName,
      collectorName);
  static String[] files = new String[12];

  static {
    Calendar now = Calendar.getInstance();
    now.add(Calendar.MINUTE, -(files.length));
    Date startTime = now.getTime();
    now.setTime(startTime);
    for (int i = 0; i < 12; i++) {
      startTime = now.getTime();
      files[i] = CollectorStreamReader.getCollectorFileName(testStream,
          startTime);
      LOG.debug("file:" + i + " :" + files[i]);
      now.add(Calendar.MINUTE, 1);
    }
  }

  static String constructMessage(int index) {
    StringBuffer str = new StringBuffer();
    str.append(index).append("Message");
    return str.toString();
  }

  static void createEmptyFile(FileSystem fs, Path parent, String fileName)
      throws IOException {
    FSDataOutputStream out = fs.create(new Path(parent, fileName));
    LOG.info("Created empty file:" + new Path(parent, fileName));
    out.close();
  }

  static void createMessageFile(String fileName, FileSystem fs, Path parent,
      int msgIndex) throws IOException {
    FSDataOutputStream out = fs.create(new Path(parent, fileName));
    for (int i = 0; i < 100; i++) {
      out.write(Base64.encodeBase64(constructMessage(msgIndex).getBytes()));
      out.write('\n');
      msgIndex++;
    }
    out.close();
    LOG.info("Created file:" + new Path(parent, fileName));
  }

  static void moveFileToStreamLocal(FileSystem fs, String streamName,
      String collectorName, Cluster cluster, Path collectorDir,
      String collectorfileName)
          throws Exception {
    Path copyPath = copyFileToStreamLocal(fs, streamName, collectorName,
        cluster, collectorDir, collectorfileName);
    fs.delete(copyPath, true);
  }

  static Path copyFileToStreamLocal(FileSystem fs, String streamName,
      String collectorName, Cluster cluster, Path collectorDir,
      String collectorfileName)
          throws Exception {
    String localStreamFileName = LocalStreamReader.getLocalStreamFileName(
        collectorName, collectorfileName);
    Path streamLocalDateDir = TestUtil.getDateDirForCollectorFile(cluster,
        streamName, collectorfileName);
    Path targetFile = new Path(streamLocalDateDir, localStreamFileName);
    Path collectorPath = new Path(collectorDir, collectorfileName);
    FileUtil.gzip(collectorPath, targetFile, cluster.getHadoopConf());
    return collectorPath;
  }

  static void assertBuffer(String fileName, int fileNum, int startIndex,
      int numMsgs, PartitionId pid, LinkedBlockingQueue<QueueEntry> buffer)
          throws InterruptedException {
    int fileIndex = (fileNum - 1) * 100 ;
    for (int i = startIndex; i < (startIndex + numMsgs); i++) {
      QueueEntry entry = buffer.take();
      Assert.assertEquals(entry.partitionId, pid);
      Assert.assertEquals(entry.partitionChkpoint,
          new PartitionCheckpoint(fileName, i + 1));
      Assert.assertEquals(new String(entry.message.getData().array()),
          constructMessage(fileIndex + i));
    }
  }

  static void setUpCollectorDataFiles(FileSystem fs, Path collectorDir,
      String... files) throws IOException {
    int i = 0;
    for (String file : files) {
      createMessageFile(file, fs, collectorDir, i);
      i += 100;
    }
  }

  static void setUpEmptyFiles(FileSystem fs, Path collectorDir,
      String... files) throws IOException {
    for (String file : files) {
      createEmptyFile(fs, collectorDir, file);
    }
  }

  static Cluster setupLocalCluster(String className, String testStream,
      PartitionId pid, String[] collectorFiles,
      String[] emptyFiles, int numFilesToMoveToStreamLocal,
      boolean withoutSymlink) throws Exception {
    return setupCluster(className, testStream, pid, "file:///", collectorFiles,
        emptyFiles, numFilesToMoveToStreamLocal, withoutSymlink);
  }

  private static Cluster setupCluster(String className, String testStream,
      PartitionId pid, String hdfsUrl, String[] collectorFiles, 
      String[] emptyFiles, int numFilesToMoveToStreamLocal,
      boolean withoutSymlink) throws Exception {
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add(testStream);
    Map<String, String> clusterConf = new HashMap<String, String>();
    clusterConf.put("hdfsurl", hdfsUrl);
    clusterConf.put("jturl", "local");
    clusterConf.put("name", pid.getCluster());
    clusterConf.put("jobqueuename", "default");
    
    Cluster cluster = new Cluster(clusterConf, 
        "/tmp/test/databus/" + className,
         null, sourceNames);
    Path streamDir = new Path(cluster.getDataDir(), testStream);

    // setup stream and collector dirs
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    Path collectorDir = new Path(streamDir, pid.getCollector());
    fs.delete(collectorDir, true);
    fs.mkdirs(collectorDir);

    // setup data dirs
    if (collectorFiles != null) {
      TestUtil.setUpCollectorDataFiles(fs, collectorDir, collectorFiles);
    }

    if (emptyFiles != null) {
      TestUtil.setUpEmptyFiles(fs, collectorDir, emptyFiles);
    }

    if (numFilesToMoveToStreamLocal > 0 && collectorFiles != null) {
      fs.delete(new Path(cluster.getLocalFinalDestDirRoot()), true);
      for (int i = 0; i < numFilesToMoveToStreamLocal; i++) {
        TestUtil.moveFileToStreamLocal(fs, testStream, pid.getCollector(),
            cluster, collectorDir, collectorFiles[i]);
      }
    }
    
    if (!withoutSymlink) {
      writeCurrentScribeFileName(fs, collectorDir, testStream,
        collectorFiles[collectorFiles.length-1]);
    }
    return cluster;
  }

  static Cluster setupDFSCluster(String className, String testStream,
      PartitionId pid, String hdfsUrl, String[] collectorFiles,
      String[] emptyFiles, int numFilesToMoveToStreamLocal) throws Exception {
    return setupCluster(className, testStream, pid, hdfsUrl, collectorFiles,
        emptyFiles, numFilesToMoveToStreamLocal, false);
  }

  static void cleanupCluster(Cluster cluster) throws IOException {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    fs.delete(new Path(cluster.getRootDir()), true);    
  }

  static Path getLocalStreamPath(Cluster cluster, String testStream,
      String collectorName, String collectorFile) throws Exception {
    return new Path(getDateDirForCollectorFile(cluster, testStream,
        collectorFile),
        LocalStreamReader.getLocalStreamFileName(collectorName, collectorFile));
  }

  public static Path getDateDirForCollectorFile(Cluster cluster,
      String streamName, String fileName) throws Exception {    
    Date date = CollectorStreamReader.getDateFromCollectorFile(fileName);
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.MINUTE, 1);
    return TestUtil.getDateDir(cluster, streamName, cal.getTime());
  }

  public static Path getDateDir(Cluster cluster, String streamName,  Date date)
      throws Exception{
    return new Path(cluster.getLocalDestDir(streamName, date));
  }

  public static String getCollectorFileName(String localStreamFile) {
    return localStreamFile.substring(localStreamFile.indexOf('-') + 1,
        localStreamFile.indexOf('.'));  
  }

  static void writeCurrentScribeFileName(FileSystem fs,
      Path collectorDir, String streamName, String currentScribeFile)
      throws IOException {
    FSDataOutputStream scribe = fs.create(
        new Path(collectorDir, streamName + "_current"));
    scribe.write(currentScribeFile.getBytes());
    scribe.write('\n');
    scribe.close();
    LOG.info("Written current scribe file name:" + currentScribeFile);
  }

  static void writeEmptyCurrentScribeFile(FileSystem fs, Path collectorDir,
      String streamName) throws IOException {
    FSDataOutputStream scribe = fs.create(
        new Path(collectorDir, streamName + "_current"));
    scribe.close();
  }
}
