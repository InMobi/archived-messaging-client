package com.inmobi.messaging.consumer.util;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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
import com.inmobi.messaging.ClientConfig;

import com.inmobi.messaging.consumer.databus.Checkpoint;
import com.inmobi.messaging.consumer.databus.CheckpointList;
import com.inmobi.messaging.consumer.databus.DatabusConsumerConfig;
import com.inmobi.messaging.consumer.databus.StreamType;


public class CheckpointUtil implements DatabusConsumerConfig {

  private static final Log LOG = LogFactory.getLog(CheckpointUtil.class);
  
  public CheckpointUtil() {
  	
  }

  public static void prepareCheckpointList(String superKey,
      CheckpointProvider provider,
      Set<Integer> idList, Path streamDir, CheckpointList checkpointList) 
      		throws IOException {

    Checkpoint oldCheckpoint = null;
    byte[] chkpointData = provider.read(superKey);
    if (chkpointData != null) {
      oldCheckpoint = new Checkpoint(chkpointData);
    }

    if (oldCheckpoint == null) {
      LOG.info("Old checkpoint is not available nothing to prepare");
      return;
    }
   
    Map<PartitionId, PartitionCheckpoint> partitionCheckpoints = 
        oldCheckpoint.getPartitionsCheckpoint();
    for (Map.Entry<PartitionId, PartitionCheckpoint> entry : 
      partitionCheckpoints.entrySet()) {
    	System.out.println("pck " + entry.getValue());
    	Path tmpPathFile = new Path(entry.getValue().getStreamFile().toString()).
    			getParent();
    	//to get streamDir path  form streamDirPath/YYYY/MM/DD/HH/MN)
    	for (int i = 0; i < 5; i++) {
    		tmpPathFile = tmpPathFile.getParent();
    	}
    	System.out.println("streamdir " + streamDir + "  tmpPathFile "+ tmpPathFile);
    	streamDir = streamDir.getParent().getParent();
    	if ((streamDir).compareTo(tmpPathFile) == 0) {
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
    }
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
  
  protected static CheckpointProvider createCheckpointProvider(
  	String checkpointProviderClassName, String chkpointDir) {
  	CheckpointProvider chkProvider = null;
  	try {
  		Class<?> clazz = Class.forName(checkpointProviderClassName);
  		Constructor<?> constructor = clazz.getConstructor(String.class);
  		chkProvider = (CheckpointProvider) constructor.newInstance(new Object[]
  				{chkpointDir});
  	} catch (Exception e) {
  		throw new IllegalArgumentException("Could not create checkpoint provider "
  				+ checkpointProviderClassName, e);
  	}
  	return chkProvider;
  }

  
  public static void main(String [] args) throws Exception {
  	String confFile = args[0];
  	ClientConfig config;
  	CheckpointProvider checkpointProvider;
  	Set<Integer> idList = new TreeSet<Integer>();
  	config = ClientConfig.load(confFile);
  	String chkpointProviderClassName = config.getString(
        chkProviderConfig, DEFAULT_CHK_PROVIDER);
    String databusCheckpointDir = config.getString(checkpointDirConfig, 
        DEFAULT_CHECKPOINT_DIR);
    checkpointProvider = createCheckpointProvider(
        chkpointProviderClassName, databusCheckpointDir);
    
    for (int i = 0; i < 60; i++) {
    	idList.add(i);
    }
    String topicName = config.getString("topic.name", null);
    String consumerName = config.getString("consumer.name", null);
    String type = config.getString(databusStreamType, DEFAULT_STREAM_TYPE);
    String [] databusRootDir = (config.getString(databusRootDirsConfig)).split(",");
    StreamType streamType = StreamType.valueOf(type);
    String superKey = consumerName + "_" + topicName;
    CheckpointList checkpointList = new CheckpointList(idList);
    for (String databusRootDirPath : databusRootDir) {
    	Path streamDir = DatabusUtil.getStreamDir(streamType, 
    			new Path(databusRootDirPath), topicName);
    	CheckpointUtil.prepareCheckpointList(superKey, checkpointProvider, idList,
    			streamDir, checkpointList);
    }
    checkpointList.write(checkpointProvider, superKey);
    checkpointList.read(checkpointProvider, superKey);
  }
}


