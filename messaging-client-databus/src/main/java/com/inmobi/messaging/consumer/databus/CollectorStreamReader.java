package com.inmobi.messaging.consumer.databus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.Cluster;

class CollectorStreamReader extends StreamReader {

  private static final Log LOG = LogFactory.getLog(CollectorStreamReader.class);

  private final Path collectorDir;
  private PathFilter pathFilter;
  private long waitTimeForFlush;
  private boolean withoutSymlink = false;

  CollectorStreamReader(PartitionId partitionId,
      Cluster cluster, String streamName, long waitTimeForFlush) {
    this(partitionId, cluster, streamName, waitTimeForFlush, false);
  }

  CollectorStreamReader(PartitionId partitionId,
      Cluster cluster, String streamName, long waitTimeForFlush,
      boolean withoutSymlink) {
    Path streamDir = new Path(cluster.getDataDir(), streamName);
    this.collectorDir = new Path(streamDir, partitionId.getCollector());
    this.waitTimeForFlush = waitTimeForFlush;
    this.withoutSymlink = withoutSymlink;
    super.init(partitionId, cluster, streamName);
    pathFilter = new ScribePathFilter();
    LOG.info("Collector reader initialized with partitionId:" + partitionId +
        " streamDir:" + streamDir + 
        " collectorDir:" + collectorDir +
        " waitTimeForFlush:" + waitTimeForFlush);
  }

  void build() throws IOException {
    files = new TreeMap<String, Path>();
    LOG.info("Building file list");
    if (fs.exists(collectorDir)) {
      FileStatus[] fileStatuses = fs.listStatus(collectorDir, pathFilter);
      if (fileStatuses == null || fileStatuses.length == 0) {
        LOG.info("No files in directory:" + collectorDir);
        return;
      }
      for (FileStatus file : fileStatuses) {
        LOG.debug("Adding Path:" + file.getPath());
        files.put(file.getPath().getName(), file.getPath());
      }
    }
    fileNameIterator = files.navigableKeySet().iterator();
  }

  @Override
  protected Path getFileForCheckpoint(PartitionCheckpoint checkpoint)
      throws Exception {
    if (isCollectorFile(checkpoint.getFileName())) {
      Path checkpointPath = new Path(collectorDir, checkpoint.getFileName());
      if (fs.exists(checkpointPath)) {
        return checkpointPath;
      }
    }
    return null;
  }

  @Override
  protected BufferedReader createReader(FSDataInputStream in)
      throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    return reader;
  }


  @Override
  protected Path getFileForTimeStamp(Date startTime)
      throws Exception {
    Map.Entry<String, Path> ceilingEntry = files.ceilingEntry(
        getCollectorFileName(streamName, timestamp));
    if (ceilingEntry != null) {
      return ceilingEntry.getValue();
    } else {
      return null;
    }
  }

  String readLine() throws Exception {
    String line = null;
    if (inStream != null) {
      line = readLine(inStream, reader);
    }
    while (line == null) { // reached end of file?
      // see if currentFile is current file is Scribe file
      if (isCurrentScribeFile()) {
        LOG.info("waiting for current file to be flushed");
        waitForFlushAndReOpen();
        LOG.info("Reading from same file after reopen");
      } else {
        build(); // rebuild file list
        if (!setIterator()) {
          LOG.info("Could not find current file in the stream");
          if (stillInCollectorStream()) {
            LOG.info("Staying in collector stream as earlier files still exist");
            if (!setNextHigher(currentFile.getName())) {
              LOG.info("Could not find next higher file.");
              if (waitForScribeCurrentFileCreation()) {
                LOG.info("Reading from the current written file");
              } else {
                return null;
              }
            } else {
              LOG.info("Reading from next higher file");
            }
          } else {
            LOG.info("Current file would have been moved to Local Stream");
            return null;
          }
        } else if (!nextFile()) { //there is no next file
          if (waitForScribeCurrentFileCreation()) {
            LOG.info("Reading from the current written file");
          } else {
            return null;
          }
        } else {
          LOG.info("Reading from next file: " + currentFile);
        }
      }
      line = readLine(inStream, reader);
    }
    return line;
  }

  void waitForFlushAndReOpen() throws Exception {
    Thread.sleep(waitTimeForFlush);
    openCurrentFile(false);    
  }

  boolean waitForScribeCurrentFileCreation() throws Exception {
    String fileName = getCurrentScribeFile();
    if (fileName == null) {
      return false;
    }
    Path path = new Path(collectorDir, fileName);
    while (!fs.exists(path)) {
      LOG.info("Waiting for the current file to be created.");
      Thread.sleep(10);
    }
    build();
    if (setNextHigher(currentFile.getName())) {
      return true;
    } else {
      LOG.info("Could not find the current written file");
      return false;
    }
  }

  boolean stillInCollectorStream() throws IOException {
    Map.Entry<String, Path> firstEntry = getFirstEntry();
    if (firstEntry != null && 
        getFirstEntry().getKey().compareTo(currentFile.getName()) < 1) {
      return true;
    }
    return false;
  }

  boolean isCollectorFile(String fileName) {
    return fileName.startsWith(streamName);
  }

  final static class ScribePathFilter implements PathFilter {

    ScribePathFilter() {
    }

    @Override
    public boolean accept(Path p) {
      if (p.getName().endsWith("current")
          || p.getName().equals("scribe_stats")) {
        return false;
      }
      return true;
    }
  }

  private boolean isCurrentScribeFile() throws Exception {
    if (currentFile.getName().equals(getCurrentScribeFile())) {
      return true;
    } else {
      return false;
    }
  }

  private String getCurrentScribeFile() throws Exception {
    Path currentScribeFile = new Path(collectorDir, streamName + "_current");
    String currentFileName = null;
    while (currentFileName == null) {
      if (fs.exists(currentScribeFile)) {
        FSDataInputStream in = fs.open(currentScribeFile);
        String line = new BufferedReader(new InputStreamReader(in)).readLine();
        if (line != null) {
          currentFileName = line.trim();
        } else {
          LOG.info("Waiting for data in _current file");
          Thread.sleep(10);
        }
        in.close();        
      } else {
        LOG.info("currentScribeFile:" + currentScribeFile + " does not exist");
        if (withoutSymlink) {
          break; 
        }
        Thread.sleep(10);
      }
    }
    LOG.debug("Current scribe file name:" + currentFileName);
    return currentFileName;
  }

  static String getCollectorFileName(String collector,
      String localStreamfile) {
    String path= localStreamfile.split(collector)[1];
    String str = path.substring(StreamReader.getIndexOf(path, '-', 1) + 1,
        StreamReader.getIndexOf(path, '.', 1));
    LOG.debug("Collector file name:" + str);
    return str;
  }

  
  public static Date getDateFromCollectorFile(String fileName)
      throws Exception {
    return StreamReader.getDate(fileName, 1);
  }

  static String getCollectorFileName(String streamName, Date date) {
    return streamName + "-" +  fileFormat.get().format(date) + "_00000" ;  
  }
}
