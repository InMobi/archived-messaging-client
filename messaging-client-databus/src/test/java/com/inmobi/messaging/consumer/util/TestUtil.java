package com.inmobi.messaging.consumer.util;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.testng.Assert;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.databus.utils.FileUtil;
import com.inmobi.messaging.consumer.databus.QueueEntry;

public class TestUtil {
  static final Log LOG = LogFactory.getLog(TestUtil.class);

  private static final String testStream = "testclient";

  public static String[] files = new String[12];
  private static int increment = 1;

  static {
    Calendar now = Calendar.getInstance();
    now.add(Calendar.MINUTE, - (files.length + 5));
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

  public static void createEmptyFile(FileSystem fs, Path parent, String fileName)
      throws IOException {
    FSDataOutputStream out = fs.create(new Path(parent, fileName));
    LOG.debug("Created empty file:" + new Path(parent, fileName));
    out.close();
  }

  public static void incrementCommitTime() {
    increment++;
  }

  public static Path moveFileToStreamLocal(FileSystem fs, String streamName,
      String collectorName, Cluster cluster, Path collectorDir,
      String collectorfileName)
          throws Exception {
    return moveCollectorFile(fs, streamName, collectorName, cluster,
        collectorDir, collectorfileName, false);
  }

  public static Path moveFileToStreams(FileSystem fs, String streamName,
      String collectorName, Cluster cluster, Path collectorDir,
      String collectorfileName)
          throws Exception {
    return moveCollectorFile(fs, streamName, collectorName, cluster,
        collectorDir, collectorfileName, true);
  }

  public static Path moveCollectorFile(FileSystem fs, String streamName,
      String collectorName, Cluster cluster, Path collectorDir,
      String collectorfileName, boolean finalDir)
          throws Exception {
    Path targetFile = getTargetPath(streamName, collectorName, cluster,
        collectorfileName, finalDir);
    Path srcPath = copyCollectorFile(targetFile, cluster, collectorDir,
        collectorfileName);
    fs.delete(srcPath, true);
    return targetFile;
  }

  private static Path getTargetPath(String streamName,
      String collectorName, Cluster cluster, 
      String collectorfileName, boolean finalDir) throws IOException {
    String streamFileName = LocalStreamCollectorReader.getDatabusStreamFileName(
        collectorName, collectorfileName);
    
    Path streamDir;
    if (finalDir) {
      streamDir = TestUtil.getDateFinalDirForCollectorFile(cluster,
        streamName, collectorfileName);
    } else {
      streamDir = TestUtil.getDateLocalDirForCollectorFile(cluster,
          streamName, collectorfileName);
    }
    return new Path(streamDir, streamFileName);
  }

  private static Path copyCollectorFile(Path targetFile, Cluster cluster, 
      Path collectorDir, String collectorfileName) throws IOException {
    Path collectorPath = new Path(collectorDir, collectorfileName);
    FileUtil.gzip(collectorPath, targetFile, cluster.getHadoopConf());
    LOG.info("Copied " + collectorPath + " to" + targetFile);
    return collectorPath;
  }

  public static Path copyFileToStreamLocal(FileSystem fs, String streamName,
      String collectorName, Cluster cluster, Path collectorDir,
      String collectorfileName)
          throws Exception {
    Path targetFile = getTargetPath(streamName, collectorName, cluster,
        collectorfileName, false);

    copyCollectorFile(targetFile, cluster, collectorDir, collectorfileName);
    return targetFile;
  }

  public static Path copyFileToStreams(FileSystem fs, String streamName,
      String collectorName, Cluster cluster, Path collectorDir,
      String collectorfileName)
          throws Exception {
    Path targetFile = getTargetPath(streamName, collectorName, cluster,
        collectorfileName, true);

    copyCollectorFile(targetFile, cluster, collectorDir, collectorfileName);
    return targetFile;
  }

  public static void assertBuffer(StreamFile file, int fileNum, int startIndex,
      int numMsgs, PartitionId pid, LinkedBlockingQueue<QueueEntry> buffer,
      boolean isDatabusData)
          throws InterruptedException, IOException {
    int fileIndex = (fileNum - 1) * 100 ;
    for (int i = startIndex; i < (startIndex + numMsgs); i++) {
      QueueEntry entry = buffer.take();
      Assert.assertEquals(entry.getPartitionId(), pid);
      Assert.assertEquals(entry.getPartitionChkpoint(),
          new PartitionCheckpoint(file, i + 1));
      if (isDatabusData) {
        Assert.assertEquals(new String(entry.getMessage().getData().array()),
          MessageUtil.constructMessage(fileIndex + i));
      } else {
        Assert.assertEquals(MessageUtil.getTextMessage(
            entry.getMessage().getData().array()),
            new Text(MessageUtil.constructMessage(fileIndex + i)));
      }
    }
  }

  public static void setUpCollectorDataFiles(FileSystem fs, Path collectorDir,
      String... files) throws IOException {
    int i = 0;
    for (String file : files) {
      MessageUtil.createMessageFile(file, fs, collectorDir, i);
      i += 100;
    }
  }

  public static void setUpEmptyFiles(FileSystem fs, Path collectorDir,
      String... files) throws IOException {
    for (String file : files) {
      createEmptyFile(fs, collectorDir, file);
    }
  }

  public static Cluster setupLocalCluster(String className, String testStream,
      PartitionId pid, String[] collectorFiles,
      String[] emptyFiles, Path[] databusFiles, 
      int numFilesToMoveToStreamLocal) throws Exception {
    return setupCluster(className, testStream, pid, "file:///", collectorFiles,
        emptyFiles, databusFiles, numFilesToMoveToStreamLocal, 0);
  }

  public static Cluster setupLocalCluster(String className, String testStream,
      PartitionId pid, String[] collectorFiles,
      String[] emptyFiles,  
      int numFilesToMoveToStreamLocal) throws Exception {
    return setupCluster(className, testStream, pid, "file:///", collectorFiles,
        emptyFiles, null, numFilesToMoveToStreamLocal, 0);
  }

  public static Cluster setupLocalCluster(String className, String testStream,
      PartitionId pid, String[] collectorFiles,
      String[] emptyFiles, Path[] databusFiles, 
      int numFilesToMoveToStreamLocal, int numFilesToMoveToStreams)
          throws Exception {
    return setupCluster(className, testStream, pid, "file:///", collectorFiles,
        emptyFiles, databusFiles, numFilesToMoveToStreamLocal,
        numFilesToMoveToStreams);
  }

  public static Path getCollectorDir(Cluster cluster, String streamName,
      String collectorName) {
    Path streamDir = new Path(cluster.getDataDir(), streamName);
    return new Path(streamDir, collectorName);
  }

  private static Cluster setupCluster(String className, String testStream,
      PartitionId pid, String hdfsUrl, String[] collectorFiles, 
      String[] emptyFiles, Path[] databusFiles, 
      int numFilesToMoveToStreamLocal, int numFilesToMoveToStreams)
          throws Exception {
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

    // setup stream and collector dirs
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    Path collectorDir = getCollectorDir(cluster, testStream, pid.getCollector());
    fs.delete(collectorDir, true);
    fs.delete(new Path(cluster.getLocalFinalDestDirRoot()), true);
    fs.delete(new Path(cluster.getFinalDestDirRoot()), true);
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
        Path movedPath = TestUtil.moveFileToStreamLocal(fs,
            testStream, pid.getCollector(),
            cluster, collectorDir, collectorFiles[i]);
        if (databusFiles != null) {
          databusFiles[i] = movedPath;
        }
      }
    }
    
    if (numFilesToMoveToStreams > 0 && collectorFiles != null) {
      fs.delete(new Path(cluster.getFinalDestDirRoot()), true);
      for (int i = 0; i < numFilesToMoveToStreams; i++) {
        Path movedPath = TestUtil.moveFileToStreams(fs,
            testStream, pid.getCollector(),
            cluster, collectorDir, collectorFiles[i]);
        if (databusFiles != null) {
          databusFiles[i] = movedPath;
        }
      }
    }

    return cluster;
  }

  public static void setUpFiles(Cluster cluster, String collectorName, 
      String[] collectorFiles, 
      String[] emptyFiles, Path[] databusFiles, 
      int numFilesToMoveToStreamLocal, int numFilesToMoveToStreams)
          throws Exception {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    Path streamDir = new Path(cluster.getDataDir(), testStream);
    Path collectorDir = new Path(streamDir, collectorName);

    // setup data dirs
    if (collectorFiles != null) {
      TestUtil.setUpCollectorDataFiles(fs, collectorDir, collectorFiles);
    }

    if (emptyFiles != null) {
      TestUtil.setUpEmptyFiles(fs, collectorDir, emptyFiles);
    }

    if (numFilesToMoveToStreamLocal > 0 && collectorFiles != null) {
      for (int i = 0; i < numFilesToMoveToStreamLocal; i++) {
        if (numFilesToMoveToStreams > 0) {
          TestUtil.copyFileToStreamLocal(fs,
              testStream, collectorName,
              cluster, collectorDir, collectorFiles[i]);
        } else {
          Path movedPath = TestUtil.moveFileToStreamLocal(fs,
              testStream, collectorName,
              cluster, collectorDir, collectorFiles[i]);
          if (databusFiles != null) {
            databusFiles[i] = movedPath;
          }
        }
      }
    }
    
    if (numFilesToMoveToStreams > 0 && collectorFiles != null) {
      for (int i = 0; i < numFilesToMoveToStreams; i++) {
        Path movedPath = TestUtil.moveFileToStreams(fs,
            testStream, collectorName,
            cluster, collectorDir, collectorFiles[i]);
        if (databusFiles != null) {
          databusFiles[i] = movedPath;
        }
      }
    }    
  }

  public static Cluster setupDFSCluster(String className, String testStream,
      PartitionId pid, String hdfsUrl, String[] collectorFiles,
      String[] emptyFiles, Path[] databusFiles, 
      int numFilesToMoveToStreamLocal, int numFilesToMoveToStreams)
          throws Exception {
    return setupCluster(className, testStream, pid, hdfsUrl, collectorFiles,
        emptyFiles, databusFiles, numFilesToMoveToStreamLocal,
        numFilesToMoveToStreams);
  }
  
  public static Cluster setupDFSCluster(String className, String testStream,
      PartitionId pid, String hdfsUrl, String[] collectorFiles,
      String[] emptyFiles, int numFilesToMoveToStreamLocal) throws Exception {
    return setupDFSCluster(className, testStream, pid, hdfsUrl, collectorFiles,
        emptyFiles, null, numFilesToMoveToStreamLocal, 0);
  }

  public static void cleanupCluster(Cluster cluster) throws IOException {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    fs.delete(new Path(cluster.getRootDir()), true);    
  }

  static Date getCommitDateForCollectorFile(String fileName)
      throws IOException {
    Calendar cal = Calendar.getInstance();
    Date date = CollectorStreamReader.getDateFromCollectorFile(fileName);
    cal.setTime(date);
    cal.add(Calendar.MINUTE, increment);
    return cal.getTime();
  }

  public static Path getDateLocalDirForCollectorFile(Cluster cluster,
      String streamName, String fileName) throws IOException {
    return TestUtil.getDateLocalDir(cluster, streamName,
        getCommitDateForCollectorFile(fileName));
  }

  private static Path getDateFinalDirForCollectorFile(Cluster cluster,
      String streamName, String fileName) throws IOException {
    return TestUtil.getDateFinalDir(cluster, streamName, 
        getCommitDateForCollectorFile(fileName));
  }

  public static Path getDateLocalDir(Cluster cluster, String streamName,
      Date date) throws IOException {
    return new Path(cluster.getLocalDestDir(streamName, date));
  }

  private static Path getDateFinalDir(Cluster cluster, String streamName,
      Date date) throws IOException{
    return new Path(cluster.getFinalDestDir(streamName, date.getTime()));
  }
}
