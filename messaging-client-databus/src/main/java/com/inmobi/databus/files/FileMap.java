package com.inmobi.databus.files;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;

public abstract class FileMap<T extends StreamFile> {
  private static final Log LOG = LogFactory.getLog(FileMap.class);

  protected TreeMap<T, FileStatus> files;
  protected Iterator<T> fileNameIterator;
  protected PathFilter pathFilter;

  public FileMap() {
    this.pathFilter = createPathFilter();
  }

  protected abstract TreeMap<T, FileStatus> createFilesMap();

  protected abstract T getStreamFile(FileStatus file);

  protected abstract T getStreamFile(String fileName);

  protected abstract PathFilter createPathFilter(); 

  protected abstract void buildList() throws IOException;


  private void createIterator() {
    fileNameIterator = files.navigableKeySet().iterator();    
  }

  public void addPath(FileStatus path) {
    T fileKey = getStreamFile(path);
    files.put(fileKey, path);
    LOG.info("Added path: " + path.getPath() + "timestamp [" + path
    .getModificationTime() + "]");
  }

  public FileStatus getCeilingValue(T fileKey) {
    Map.Entry<T, FileStatus> ceilingEntry = files.ceilingEntry(fileKey);
    if (ceilingEntry != null) {
      return ceilingEntry.getValue();
    } else {
      return null;
    }
  }

  public FileStatus getHigherValue(FileStatus file) {
    T fileKey = getStreamFile(file);
    return getHigherValue(fileKey);
  }

  public FileStatus getHigherValue(String fileName) {
    T fileKey = getStreamFile(fileName);
    return getHigherValue(fileKey);
  }

  private FileStatus getHigherValue(T fileKey) {
    Map.Entry<T, FileStatus> higherEntry = files.higherEntry(fileKey);
    if (higherEntry != null) {
      return higherEntry.getValue();
    } else {
      return null;
    }
  }

  public FileStatus getValue(String fileName) {
    T fileKey = getStreamFile(fileName);
    return files.get(fileKey);
  }

  public FileStatus getValue(StreamFile fileKey) {
    return files.get(fileKey);
  }

  public void build() throws IOException {
    files = createFilesMap();
    buildList();
    createIterator();
  }

  public boolean isEmpty() {
    return files.isEmpty();
  }

  public FileStatus getFirstFile() {
    Map.Entry<T, FileStatus> first = files.firstEntry();
    if (first != null) {
      return first.getValue();
    }
    return null;
  }

  private Map.Entry<T, FileStatus> getFirstEntry() {
    return files.firstEntry();
  }

  public boolean containsFile(String fileName) {
    return files.containsKey(getStreamFile(fileName)); 
  }

  public boolean setIterator(FileStatus cfile) {
    createIterator();
    T file = getStreamFile(cfile);
    if (cfile != null) {
      while (fileNameIterator.hasNext()) {
        StreamFile nextfile = fileNameIterator.next();
        if (nextfile.equals(file)) {
          return true;
        } 
      }
    } 
    LOG.info("Did not find file" + cfile.getPath());
    return false;
  }

  public boolean isBefore(String fileName) {
    if (!isEmpty()
        && getFirstFile().getPath().getName().compareTo(fileName) > 0) {
      return true;
    }
    return false;
  }

  public boolean isBefore(T streamFile) {
    if (!isEmpty() && getFirstEntry().getKey().compareTo(streamFile) > 0) {
      return true;
    }
    return false;
  }

  public boolean isWithin(String fileName) {
    if (!isEmpty()
        && getFirstFile().getPath().getName().compareTo(fileName) < 1) {
      return true;
    }
    return false;
  }

  public FileStatus getNext() {
    if (fileNameIterator.hasNext()) {
      StreamFile fileName = fileNameIterator.next();
      LOG.debug("next file name:" + fileName);
      return getValue(fileName);
    }
    return null;
  }

  public boolean hasNext() {
    return fileNameIterator.hasNext();
  }

}
