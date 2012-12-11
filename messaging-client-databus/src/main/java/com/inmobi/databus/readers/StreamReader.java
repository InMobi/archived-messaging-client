package com.inmobi.databus.readers;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public abstract class StreamReader<T extends StreamFile> {

  private static final Log LOG = LogFactory.getLog(StreamReader.class);

  protected Date timestamp;
  protected PartitionCheckpoint checkpoint;
  protected PartitionId partitionId;
  protected FileStatus currentFile;
  protected long currentLineNum = 0;
  protected FileSystem fs;
  protected volatile boolean closed = false;
  protected boolean noNewFiles = false; // this is purely for tests
  private long waitTimeForCreate;
  protected Path streamDir;
  protected final PartitionReaderStatsExposer metrics;
  private FileMap<T> fileMap;

  protected StreamReader(PartitionId partitionId, FileSystem fs, 
      Path streamDir, long waitTimeForCreate,
      PartitionReaderStatsExposer metrics, boolean noNewFiles)
          throws IOException {
    this.partitionId = partitionId;
    this.fs = fs;
    this.streamDir = streamDir;
    this.waitTimeForCreate = waitTimeForCreate;
    this.metrics = metrics;
    this.noNewFiles = noNewFiles;
    this.fileMap = createFileMap();
  }
  
  public boolean initFromNextCheckPoint() throws IOException {
  	//The method is overridden in the DatabusStreamWaitingReader
  	return true;
  }
  
  public boolean prepareMoveToNext(FileStatus currentFile, FileStatus nextFile)
  		throws IOException {
  	return true;
  }

  public void openStream() throws IOException {
    openCurrentFile(false);
  }

  public void closeStream() throws IOException {
    closeCurrentFile();    
  }

  public void close() throws IOException {
    closeStream();
    closed = true;
  }

  protected abstract FileMap<T> createFileMap() throws IOException;

  public void build() throws IOException {
    fileMap.build();
  }

  protected boolean setIterator() {
    return fileMap.setIterator(currentFile);
  }

  protected abstract void openCurrentFile(boolean next) throws IOException;
  
  protected abstract void closeCurrentFile() throws IOException; 
  protected void initCurrentFile() {
    currentFile = null;
    resetCurrentFile();    
  }

  public boolean initializeCurrentFile(Date timestamp) throws IOException {
    initCurrentFile();
    this.timestamp = timestamp;
    T file = getStreamFile(timestamp);
    LOG.debug("Stream file corresponding to timestamp:" + timestamp +
        " is " + file);
    currentFile = fileMap.getCeilingValue(file);

    if (currentFile != null) {
      setIterator();
      LOG.debug("CurrentFile:" + getCurrentFile() + " currentLineNum:"+ 
          currentLineNum);
    } else {
      LOG.info("Did not find stream file for timestamp:" + timestamp);
    }
    return currentFile != null;
  }

  public boolean initializeCurrentFile(PartitionCheckpoint checkpoint)
      throws IOException {
    initCurrentFile();
    this.checkpoint = checkpoint;
    LOG.debug("checkpoint:" + checkpoint);
    currentFile = fileMap.getValue(checkpoint.getStreamFile());
    if (currentFile != null) {
      currentLineNum = checkpoint.getLineNum();
      LOG.debug("CurrentFile:" + getCurrentFile() + " currentLineNum:" + 
          currentLineNum);
      setIterator();
    } 
    return currentFile != null;
  }

  public boolean initFromStart() throws IOException {
    initCurrentFile();
    currentFile = fileMap.getFirstFile();

    if (currentFile != null) {
      LOG.debug("CurrentFile:" + getCurrentFile() + " currentLineNum:" + 
          currentLineNum);
      setIterator();
    }
    return currentFile != null;
  }

  protected void resetCurrentFile() {
    currentFile = null;
    resetCurrentFileSettings();
  }

  public boolean isEmpty() {
    return fileMap.isEmpty();
  }

  protected FileStatus getHigherValue(FileStatus file) throws IOException {
    return fileMap.getHigherValue(file);
  }

  protected boolean setIteratorToFile(FileStatus file)
      throws IOException {
    if (file != null) {
      currentFile = file;
      resetCurrentFileSettings();
      setIterator();
      return true;
    }
    return false;
  }

  protected boolean setNextHigher(String currentFileName) throws IOException {
    LOG.debug("finding next higher for " + currentFileName);
    FileStatus nextHigherFile  = fileMap.getHigherValue(currentFileName);
    return setIteratorToFile(nextHigherFile);
  }

  public Path getCurrentFile() {
    if (currentFile == null)
      return null;
    return currentFile.getPath();
  }

  public T getCurrentStreamFile() {
    if (currentFile == null) {
      return null;
    }
    return getStreamFile(currentFile);
  }

  public long getCurrentLineNum() {
    return currentLineNum;
  }

  protected abstract T getStreamFile(Date timestamp);

  protected abstract T getStreamFile(FileStatus status);

  /** 
   * Returns null when reached end of stream 
   */
  public abstract byte[] readLine() throws IOException, InterruptedException;

  protected abstract byte[] readRawLine() throws IOException;

  /**
   * Skip the number of lines passed.
   * 
   * @return the actual number of lines skipped.
   */
  protected long skipLines(long numLines) throws IOException {
    long lineNum = 0;
    while (lineNum != numLines) {
      byte[] line = readRawLine();
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

  /**
   * Read the next line in the current file. 
   * @return Null if end of file is reached, the line itself if read successfully
   * 
   * @throws IOException
   */
  protected byte[] readNextLine() throws IOException {
    long start = System.nanoTime();
    byte[] line = readRawLine();
    if (line != null) {
      long end = System.nanoTime();
      currentLineNum++;
      metrics.incrementMessagesReadFromSource();
      metrics.addCumulativeNanosFetchMessage(end - start);
    }
    return line;
  }


  protected void resetCurrentFileSettings() {
    currentLineNum = 0;
  }

  protected boolean nextFile() throws IOException {
    if (hasNextFile()) {
    	setNextFile();
    	return true;
    }
    return false;
  }

  protected void setNextFile() throws IOException {
  	FileStatus nextFile = fileMap.getNext();
    if (nextFile != null) {
    	boolean next = prepareMoveToNext(currentFile, nextFile);
      currentFile = nextFile;
      openCurrentFile(next);
    }
  }

  protected boolean hasNextFile() throws IOException {
    LOG.debug("In next file");
    if (!setIterator()) {
      LOG.info("could not set iterator for currentfile");
      return false;
    }
    if (fileMap.hasNext()) {
      LOG.debug("Next file available");
      return true;
    }
    LOG.debug("No next file available");
    return false;
  }

  public boolean setCurrentFile(String streamFileName, 
      long currentLineNum) throws IOException {
    if (fileMap.containsFile(streamFileName)) {
      currentFile = fileMap.getValue(streamFileName);
      setIterator();
      this.currentLineNum = currentLineNum;
      LOG.info("Set current file:" + getCurrentFile() +
          "currentLineNum:" + currentLineNum);
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

  public void startFromTimestmp(Date timestamp) throws IOException,
      InterruptedException {
    if (!initializeCurrentFile(timestamp)) {
      if (noNewFiles) {
        // this boolean check is only for tests 
        return;
      }
      waitForNextFileCreation(timestamp);
    }
  }

  public void startFromBegining() throws IOException, InterruptedException {
    if (!initFromStart()) {
      if (noNewFiles) {
        // this boolean check is only for tests 
        return;
      }
      waitForNextFileCreation();
    }
  }

  protected void waitForFileCreate() throws InterruptedException {
    Thread.sleep(waitTimeForCreate);
    metrics.incrementWaitTimeUnitsNewFile();
  }

  private void waitForNextFileCreation() throws IOException,
      InterruptedException {
    while (!closed && !initFromStart()) {
      LOG.info("Waiting for next file creation");
      waitForFileCreate();
      build();
    }
  }

  private void waitForNextFileCreation(Date timestamp) throws IOException,
      InterruptedException {
    while (!closed && !initializeCurrentFile(timestamp)) {
      LOG.info("Waiting for next file creation");
      waitForFileCreate();
      build();
    }
  }

  protected boolean isBeforeStream(T streamFile) {
    return fileMap.isBefore(streamFile);
  }

  public boolean isBeforeStream(String fileName) throws IOException {
    return fileMap.isBefore(fileName);
  }
  
  protected boolean isWithinStream(String fileName) throws IOException {
    return fileMap.isWithin(fileName);
  }
  
  protected FileStatus getFirstFileInStream() {
  	return fileMap.getFirstFile();
  }
}
