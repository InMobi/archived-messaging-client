package com.inmobi.databus.files;

public class DatabusStreamFile implements StreamFile {

  private final String collectorName;
  private final CollectorFile collectorFile;
  private final String extension;
  private String parent;
 
  public DatabusStreamFile(String collectorName,
      CollectorFile collectorFile,
      String extension) {
    this.collectorName = collectorName;
    this.collectorFile = collectorFile;
    this.extension = extension;
    this.parent = null;
  }

  public DatabusStreamFile(String collectorName,
      CollectorFile collectorFile,
      String extension, String parent) {
    this.collectorName = collectorName;
    this.collectorFile = collectorFile;
    this.extension = extension;
    this.parent = parent;
  }

  public static DatabusStreamFile create(String streamName, String fileName) {
    return create(streamName, fileName, null);
  }

  public static DatabusStreamFile create(String streamName, String fileName,
      String parentDir) {
    String strs[] = fileName.split(streamName);
    if (strs.length < 2) {
      throw new IllegalArgumentException("Invalid file name:" + fileName);
    }
    if (strs[0].length() == 0) {
      throw new IllegalArgumentException("Invalid file name:" + fileName);      
    }
    String collectorName = strs[0].substring(0, strs[0].length() - 1);
    
    String cfSubString = fileName.substring(collectorName.length() + 1);
    String str2[] = cfSubString.split("\\.");
    
    if (str2.length < 2) {
      throw new IllegalArgumentException("Invalid file name:" + fileName);
    }
    CollectorFile collectorFile =  CollectorFile.create(str2[0]);
    String extension = str2[1];
    
    return new DatabusStreamFile(collectorName, collectorFile, extension,
        parentDir);
    
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((parent == null) ? 0 : parent.hashCode());
    result = prime * result + ((collectorFile == null) ? 0 : collectorFile.hashCode());
    result = prime * result + ((collectorName == null) ? 0 : collectorName.hashCode());
    result = prime * result + ((extension == null) ? 0 : extension.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DatabusStreamFile other = (DatabusStreamFile) obj;
    if (parent != null && other.parent != null) {
      if (!parent.equals(other.parent)) {
        return false;
      }
    }
    if (collectorFile == null) {
      if (other.collectorFile != null) {
        return false;
      }
    } else if (!collectorFile.equals(other.collectorFile)) {
      return false;
    }
    if (collectorName == null) {
      if (other.collectorName != null) {
        return false;
      }
    } else if (!collectorName.equals(other.collectorName)) {
      return false;
    }
    if (extension == null) {
      if (other.extension != null) {
        return false;
      }
    } else if (!extension.equals(other.extension)) {
      return false;
    }
    return true;
  }

  public String toString() {
    return collectorName + "-" + collectorFile.toString() + "." + extension;
  }

  @Override
  public int compareTo(Object o) {
    DatabusStreamFile other = (DatabusStreamFile)o;
    int pComp;
    if (parent == null || other.parent == null) {
      pComp = 0;
    } else {
      pComp = parent.compareTo(other.parent);
    }
    if (pComp == 0) {
      int cfComp = collectorFile.compareTo(other.collectorFile);
      if ( cfComp== 0) {
        int cnComp = collectorName.compareTo(other.collectorName); 
        if ( cnComp == 0) {
          return extension.compareTo(other.extension);
        } else {
          return cnComp;
        }
      } else {
        return cfComp;
      } 
    } else {
      return pComp;
    }
  }

  public CollectorFile getCollectorFile() {
    return collectorFile;
  }

}
