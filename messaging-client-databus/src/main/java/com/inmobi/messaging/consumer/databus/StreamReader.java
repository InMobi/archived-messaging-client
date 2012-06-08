package com.inmobi.messaging.consumer.databus;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.Cluster;

abstract class StreamReader {

  private static final Log LOG = LogFactory.getLog(StreamReader.class);

  protected String streamName;
  protected TreeMap<String, Path> files;
  protected Iterator<String> fileNameIterator;
  protected PathFilter pathFilter;
  protected Date timestamp;
  protected PartitionCheckpoint checkpoint;
  protected Cluster cluster;
  protected PartitionId partitionId;
  protected Path currentFile;
  protected long currentLineNum = 0;
  protected long numLinesTobeSkipped;
  protected FileSystem fs;

  protected void init(PartitionId partitionId, Cluster cluster, 
      String streamName) {
    this.streamName = streamName;
    this.cluster = cluster;
    try {
      this.fs = FileSystem.get(cluster.getHadoopConf());
    } catch (IOException e) {
      throw new RuntimeException("Could not intialize FileSystem", e);
    }
  }

  protected FSDataInputStream inStream;
  protected BufferedReader reader;

  void openStream() throws IOException {
    openCurrentFile(false);
  }

  void close() throws IOException {
    closeCurrentFile();
  }

  protected void openCurrentFile(boolean next) throws IOException {
    closeCurrentFile();
    if (next) {
      resetCurrentFileSettings();
    } 
    numLinesTobeSkipped = currentLineNum;  
    LOG.info("Opening file:" + currentFile);
    LOG.debug("NumLinesTobeSkipped when opening:" + numLinesTobeSkipped);
    if (fs.exists(currentFile)) {
      inStream = fs.open(currentFile);
      reader = getReader(inStream);
    } else {
      LOG.debug("CurrentFile:" + currentFile + " does not exist");
    }
  }

  private void closeReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  private void closeCurrentFile() throws IOException {
    closeReader();
    if (inStream != null) {
      inStream.close();
      inStream = null;
    }
  }

  protected boolean setIterator() {
    fileNameIterator = files.navigableKeySet().iterator();
    if (currentFile != null) {
      while (fileNameIterator.hasNext()) {
        String file = fileNameIterator.next();
        if (currentFile.getName().equals(file)) {
          return true;
        } 
      }
    } 
    LOG.info("Did not find current file" + currentFile + " in the stream");
    return false;
  }

  boolean initializeCurrentFile(Date timestamp) throws Exception {
    resetCurrentFile();
    this.timestamp = timestamp;
    currentFile = getFileForTimeStamp(timestamp);
    if (currentFile != null) {
      setIterator();
      LOG.debug("CurrentFile:" + currentFile + " currentLineNum:"+ 
          currentLineNum);
    }
    return currentFile != null;
  }

  boolean initializeCurrentFile(PartitionCheckpoint checkpoint)
      throws Exception {
    resetCurrentFile();
    this.checkpoint = checkpoint;
    LOG.debug("checkpoint:" + checkpoint);
    currentFile = getFileForCheckpoint(checkpoint);
    if (currentFile != null) {
      currentLineNum = checkpoint.getLineNum();
      LOG.debug("CurrentFile:" + currentFile + " currentLineNum:" + 
          currentLineNum);
      setIterator();
    } 
    return currentFile != null;
  }

  boolean initFromStart() throws Exception {
    resetCurrentFile();
    currentFile = getFirstFile();

    if (currentFile != null) {
      LOG.debug("CurrentFile:" + currentFile + " currentLineNum:" + 
          currentLineNum);
      setIterator();
    }
    return currentFile != null;
  }

  void resetCurrentFile() {
    currentFile = null;
    resetCurrentFileSettings();
  }

  boolean setNextHigher(String currentFileName) throws IOException {
    LOG.debug("finding next higher for " + currentFileName);
    Map.Entry<String, Path> higherEntry = 
          files.higherEntry(currentFileName);
    if (higherEntry != null) {
      currentFile = higherEntry.getValue();
      LOG.debug("Next higher entry:" + currentFile);
      setIterator();
      openCurrentFile(true);
      return true;
    }
    return false;
  }

  Path getCurrentFile() {
    return currentFile;
  }

  long getCurrentLineNum() {
    return currentLineNum;
  }

  protected abstract Path getFileForCheckpoint(PartitionCheckpoint checkpoint)
      throws Exception;

  protected abstract Path getFileForTimeStamp(Date timestamp)
      throws Exception;

  protected abstract BufferedReader createReader(FSDataInputStream in)
      throws IOException;

  /** 
   * Returns null when reached end of stream 
   */
  abstract String readLine() throws Exception;

  /**
   * Skip the number of lines passed.
   * 
   * @return the actual number of lines skipped.
   */
  private long skipLines(FSDataInputStream in, BufferedReader reader, 
      long numLines) 
          throws IOException {
    long lineNum = 0;
    while (lineNum != numLines) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      lineNum++;
    }
    LOG.info("Skipped " + lineNum + " lines");
    if (lineNum != numLines) {
      LOG.warn("Skipped wrong number of lines");
    }
    return lineNum;
  }

  String readLine(FSDataInputStream in, BufferedReader reader)
      throws IOException {
    if (reader != null) {
      String line = reader.readLine();
      if (line != null) {
        currentLineNum++;
      }
      return line;
    }
    return null;
  }

  BufferedReader getReader(FSDataInputStream in) throws IOException {
    BufferedReader reader = createReader(in);
    skipLines(in, reader, numLinesTobeSkipped);
    return reader;
  }

  private void resetCurrentFileSettings() {
    currentLineNum = 0;
  }

  boolean nextFile() throws IOException {
    LOG.debug("In next file");
    if (!setIterator()) {
      LOG.info("could not set iterator for currentfile. setting next higher");
      return setNextHigher(currentFile.getName());
    }
    if (fileNameIterator.hasNext()) {
      String fileName = fileNameIterator.next();
      LOG.debug("next file name:" + fileName);
      currentFile = files.get(fileName);
      openCurrentFile(true);
      return true;
    }
    return false;
  }

  public boolean setCurrentFile(String streamFileName, 
      long currentLineNum) throws IOException {
    if (files.containsKey(streamFileName)) {
      currentFile = files.get(streamFileName);
      setIterator();
      this.currentLineNum = currentLineNum;
      LOG.debug("Set current file:" + currentFile +
          "currentLineNum:" + currentLineNum);
      openCurrentFile(false);
      return true;
    } else {
      LOG.info("Did not find current file." + streamFileName +
          " Trying to set next higher");
      if (!setNextHigher(streamFileName)) {
        return false;
      } else {
        return true;
      }
    }
  }

  Map.Entry<String, Path> getFirstEntry() {
    return files.firstEntry();
  }

  Path getFirstFile()
      throws IOException {
    Map.Entry<String, Path> first = files.firstEntry();
    if (first != null) {
      return first.getValue();
    }
    return null;
  }

  public static int getIndexOf(String str, int ch, int occurance) {
    int first = str.indexOf(ch);
    if (occurance == 1) {
      return first;
    } else {
      return (first + 1) + getIndexOf(str.substring(first + 1), ch,
          occurance - 1);
    }
  }

  public static Date getDate(String fileName, int occur) throws Exception {
    String dateStr = fileName.substring(
        StreamReader.getIndexOf(fileName, '-', occur) + 1,
        fileName.indexOf("_"));
    return fileFormat.get().parse(dateStr);  
  }

  static final ThreadLocal<DateFormat> fileFormat = 
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + "-" + "MM" + "-" + "dd" + "-" +
          "HH" + "-" + "mm");
    }    
  };

  static final ThreadLocal<DateFormat> minDirFormat = 
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + File.separator + "MM" +
          File.separator + "dd" + File.separator + "HH" + File.separator +"mm");
    }    
  };

  static final ThreadLocal<DateFormat> hhDirFormat = 
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + File.separator + "MM" +
          File.separator + "dd" + File.separator + "HH");
    }    
  };
}
