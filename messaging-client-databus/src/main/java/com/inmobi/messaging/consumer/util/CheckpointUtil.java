package com.inmobi.messaging.consumer.util;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.files.HadoopStreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.Checkpoint;
import com.inmobi.messaging.consumer.databus.CheckpointList;

public class CheckpointUtil {

  private static final Log LOG = LogFactory.getLog(CheckpointUtil.class);

  public static void prepareCheckpointList(String superKey,
      CheckpointProvider provider,
      List<Integer> idList, Path streamDir) throws IOException {

    Checkpoint oldCheckpoint = null;
    byte[] chkpointData = provider.read(superKey);
    if (chkpointData != null) {
      oldCheckpoint = new Checkpoint(chkpointData);
    }

    if (oldCheckpoint == null) {
      LOG.info("Old checkpoint is not available nothing to prepare");
      return;
    } 
    CheckpointList checkpointList = new CheckpointList(idList);

    // fill the entries in checkpoint list
    Map<PartitionId, PartitionCheckpoint> partitionCheckpoints = 
        oldCheckpoint.getPartitionsCheckpoint();
    for (Map.Entry<PartitionId, PartitionCheckpoint> entry : 
      partitionCheckpoints.entrySet()) {
      Map<Integer, PartitionCheckpoint> thisChkpoint =
          new TreeMap<Integer, PartitionCheckpoint>();
      Calendar chkCal = Calendar.getInstance();
      Date checkpointDate = DatabusStreamWaitingReader.getDateFromStreamDir(
          streamDir,
          new Path(entry.getValue().getStreamFile().toString()).getParent());
      chkCal.setTime(checkpointDate);
      int checkpointMin = chkCal.get(Calendar.MINUTE);
      Calendar chkPrevHrCal = Calendar.getInstance();
      chkPrevHrCal.setTime(checkpointDate);
      chkPrevHrCal.add(Calendar.MINUTE, 1);
      chkPrevHrCal.add(Calendar.HOUR, -1);
      while (chkPrevHrCal.before(chkCal)) {
        if (chkPrevHrCal.get(Calendar.MINUTE) == 0) {
          break;
        }
        Path minDir = DatabusStreamWaitingReader.getMinuteDirPath(streamDir,
            chkPrevHrCal.getTime());

        FileStatus lastElement = getLast(minDir);
        if (lastElement != null) {
          thisChkpoint.put(chkPrevHrCal.get(Calendar.MINUTE),
              new PartitionCheckpoint(new HadoopStreamFile(
                  lastElement.getPath().getParent(),
                  lastElement.getPath().getName(),
                  lastElement.getModificationTime()),
                  -1));
        }
        chkPrevHrCal.add(Calendar.MINUTE, 1);
      }        

      Calendar chkHrCal = Calendar.getInstance();
      chkHrCal.setTime(checkpointDate);
      chkHrCal.set(Calendar.MINUTE, 0);
      while (chkHrCal.before(chkCal)) {
        Path minDir = DatabusStreamWaitingReader.getMinuteDirPath(streamDir,
            chkHrCal.getTime());
        FileStatus lastElement = getLast(minDir);
        if (lastElement != null) {
          thisChkpoint.put(chkHrCal.get(Calendar.MINUTE),
              new PartitionCheckpoint(new HadoopStreamFile(
                  lastElement.getPath().getParent(),
                  lastElement.getPath().getName(),
                  lastElement.getModificationTime()),
                  -1));
        }
        chkHrCal.add(Calendar.MINUTE, 1);
      }
      thisChkpoint.put(checkpointMin, entry.getValue());
      checkpointList.set(entry.getKey(), new PartitionCheckpointList(
          thisChkpoint));
    }
    checkpointList.write(provider, superKey);
  }

  private static FileStatus getLast(final Path minDir)
      throws IOException {

    FileMap<HadoopStreamFile> fmap = new FileMap<HadoopStreamFile>() {
      @Override
      protected TreeMap<HadoopStreamFile, FileStatus> createFilesMap() {
        return new TreeMap<HadoopStreamFile, FileStatus>();
      }

      @Override
      protected HadoopStreamFile getStreamFile(String fileName) {
        throw new RuntimeException("Not implemented");
      }

      @Override
      protected HadoopStreamFile getStreamFile(FileStatus file) {
        return HadoopStreamFile.create(file);
      }

      @Override
      protected PathFilter createPathFilter() {
        return new PathFilter() {
          @Override
          public boolean accept(Path path) {
            if (path.getName().startsWith("_")) {
              return false;
            }
            return true;
          }          
        };
      }

      @Override
      protected void buildList() throws IOException {
        FileSystem fs = minDir.getFileSystem(new Configuration());
        doRecursiveListing(fs, minDir, pathFilter, this);
      }
    };

    fmap.build();

    return fmap.getLastFile();
  }

  private static void doRecursiveListing(FileSystem fs, Path dir,
      PathFilter pathFilter,
      FileMap<HadoopStreamFile> fmap) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(dir, pathFilter);
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.debug("No files in directory:" + dir);
    } else {
      for (FileStatus file : fileStatuses) {
        if (file.isDir()) {
          doRecursiveListing(fs, file.getPath(), pathFilter, fmap);
        } else {
          fmap.addPath(file);
        }
      }
    }
  }
}


