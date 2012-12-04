package com.inmobi.databus.readers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.files.CollectorFile;
import com.inmobi.databus.files.DatabusStreamFile;
import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class CollectorStreamReader extends StreamReader<CollectorFile> {

  private static final Log LOG = LogFactory.getLog(CollectorStreamReader.class);

  private long waitTimeForFlush;
  protected long currentOffset = 0;
  private boolean sameStream = false;
  protected FSDataInputStream inStream;
  protected BufferedReader reader;
  protected final String streamName;
  private boolean moveToNext = false;
  private CollectorReaderStatsExposer collectorMetrics;

  public CollectorStreamReader(PartitionId partitionId,
      FileSystem fs, String streamName, Path streamDir,
      long waitTimeForFlush,
      long waitTimeForCreate, CollectorReaderStatsExposer metrics,
      boolean noNewFiles) throws IOException {
    super(partitionId, fs, streamDir, waitTimeForCreate, metrics, noNewFiles);
    this.streamName = streamName;
    this.waitTimeForFlush = waitTimeForFlush;
    this.collectorMetrics = (CollectorReaderStatsExposer)(this.metrics);
    LOG.info("Collector reader initialized with partitionId:" + partitionId +
        " streamDir:" + streamDir + 
        " waitTimeForFlush:" + waitTimeForFlush +
        " waitTimeForCreate:" + waitTimeForCreate);
  }

  protected FileMap<CollectorFile> createFileMap() throws IOException {
    return new FileMap<CollectorFile>() {
      
      @Override
      protected PathFilter createPathFilter() {
        return new PathFilter() {
          @Override
          public boolean accept(Path p) {
            if (p.getName().endsWith("_current")
                || p.getName().endsWith("_stats")) {
              return false;
            }
            return true;
          }
        };
      }

      @Override
      protected void buildList() throws IOException {
        if (fs.exists(streamDir)) {
          FileStatus[] fileStatuses = fs.listStatus(streamDir, pathFilter);
          if (fileStatuses == null || fileStatuses.length == 0) {
            LOG.info("No files in directory:" + streamDir);
            return;
          }
          for (FileStatus file : fileStatuses) {
            addPath(file);
          }
        } else {
          LOG.info("Collector directory does not exist");
        }
      }

      @Override
      protected TreeMap<CollectorFile, FileStatus> createFilesMap() {
        return new TreeMap<CollectorFile, FileStatus>();
      }

      @Override
      protected CollectorFile getStreamFile(String fileName) {
        return CollectorFile.create(fileName);
      }

      @Override
      protected CollectorFile getStreamFile(FileStatus file) {
        return CollectorFile.create(file.getPath().getName());
      }
    };
  }

  protected void initCurrentFile() {
    super.initCurrentFile();
    sameStream = false;
  }

  protected void openCurrentFile(boolean next) throws IOException {
    closeCurrentFile();
    if (next) {
      resetCurrentFileSettings();
    } 
    LOG.info("Opening file:" + getCurrentFile() + " NumLinesTobeSkipped when" +
        " opening:" + currentLineNum);
    if (fs.exists(getCurrentFile())) {
      inStream = fs.open(getCurrentFile());
      reader = new BufferedReader(new InputStreamReader(inStream));
      skipOldData();
    } else {
      LOG.info("CurrentFile:" + getCurrentFile() + " does not exist");
    }
  }

  protected synchronized void closeCurrentFile() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    if (inStream != null) {
      inStream.close();
      inStream = null;
    }
  }

  protected byte[] readRawLine() throws IOException {
    String line = reader.readLine();
    if (line != null) {
      return line.getBytes();
    } else {
      return null;
    }
  }

  protected byte[] readNextLine()
      throws IOException {
    byte[] line = null;
    if (inStream != null) {
      line = super.readNextLine();
      currentOffset = inStream.getPos();
    }
    return line;
  }
  
  protected void resetCurrentFileSettings() {
    super.resetCurrentFileSettings();
    currentOffset = 0;
    moveToNext = false;
  }

  protected void skipOldData()
      throws IOException {
    if (sameStream) {
      LOG.info("Seeking to offset:" + currentOffset);
      inStream.seek(currentOffset);
    } else {
      skipLines(currentLineNum);
      sameStream = true;
      currentOffset = inStream.getPos();
    }
  }

  public byte[] readLine() throws IOException, InterruptedException {
    byte[] line = readNextLine();
    while (line == null) { // reached end of file?
      LOG.info("Read " + getCurrentFile() + " with lines:" + currentLineNum);
      if (closed) {
        LOG.info("Stream closed");
        break;
      }
      build(); // rebuild file list
      if (!hasNextFile()) { //there is no next file
        if (noNewFiles) {
          // this boolean check is only for tests 
          return null;
        } 
        if (!setIterator()) {
          LOG.info("Could not find current file in the stream");
          if (isWithinStream(getCurrentFile().getName())) {
            LOG.info("Staying in collector stream as earlier files still exist");
            startFromNextHigherAndOpen(getCurrentFile().getName());
            LOG.info("Reading from the next higher file");
          } else {
            LOG.info("Current file would have been moved to Local Stream");
            return null;
          }
        } else {
          waitForFlushAndReOpen();
          LOG.info("Reading from the same file after reopen");
        }
      } else {
        if (moveToNext) {
           nextFile();
          //setNextFile(nextFile);                            //should be next file
          LOG.info("Reading from next file: " + getCurrentFile());
        } else {
          LOG.info("Reading from same file before moving to next");
          // open the same file
          reOpen();
          moveToNext = true;
        }
      }
      line = readNextLine();
    }
    return line;
  }

  private void reOpen() throws IOException {
    openCurrentFile(false);    
  }

  private void waitForFlushAndReOpen() 
      throws IOException, InterruptedException {
    if (!closed) {
      LOG.info("Waiting for flush");
      Thread.sleep(waitTimeForFlush);
      collectorMetrics.incrementWaitTimeUnitsInSameFile();
      reOpen();
    }
  }

  private void startFromNextHigherAndOpen(String fileName) 
      throws IOException, InterruptedException {
    boolean ret = startFromNextHigher(fileName);
    if (ret) {
      openCurrentFile(true);
    }
  }

  public boolean startFromNextHigher(String fileName) 
      throws IOException, InterruptedException {
    if (!setNextHigher(fileName)) {
      if (noNewFiles) {
        // this boolean check is only for tests 
        return false;
      }
      waitForNextFileCreation(fileName);
    }
    return true;
  }

  private void waitForNextFileCreation(String fileName) 
      throws IOException, InterruptedException {
    while (!closed && !setNextHigher(fileName)) {
      LOG.info("Waiting for next file creation");
      waitForFileCreate();
      build();
    }
  }

  @Override
  protected CollectorFile getStreamFile(Date timestamp) {
    return getCollectorFile(streamName, timestamp);
  }

  protected CollectorFile getStreamFile(FileStatus status) {
    return getCollectorFile(status.getPath().getName());
  }

  public static boolean isCollectorFile(String fileName) {
    try {
      getCollectorFile(fileName);
    } catch (IllegalArgumentException ie) {
      return false;
    }
    return true;
  }

  public static Path getCollectorDir(Cluster cluster, String streamName,
      String collectorName) {
    Path streamDataDir = new Path(cluster.getDataDir(), streamName);
    return new Path(streamDataDir, collectorName);
  }

  public static String getCollectorFileName(String streamName,
      String localStreamfile) {
    return DatabusStreamFile.create(streamName, localStreamfile)
        .getCollectorFile().toString();
  }

  public static Date getDateFromCollectorFile(String fileName)
      throws IOException {
    return getCollectorFile(fileName).getTimestamp();
  }

  public static String getCollectorFileName(String streamName, Date date) {
    return getCollectorFile(streamName, date).toString();
  }

  public static CollectorFile getCollectorFile(String streamName, Date date) {
    return new CollectorFile(streamName, date, 0);
  }

  public static CollectorFile getCollectorFile(String fileName) {
    return CollectorFile.create(fileName);
  }
}
