package com.inmobi.messaging.consumer.hadoop;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.databus.AbstractMessagingDatabusConsumer;
import com.inmobi.messaging.consumer.databus.CheckpointList;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class HadoopConsumer extends AbstractMessagingDatabusConsumer 
    implements HadoopConsumerConfig {

  private String[] clusterNames;
  private Path[] rootDirs;
  private FileSystem[] fileSystems;
  private Configuration conf;
  private String inputFormatClassName;

  protected void initializeConfig(ClientConfig config) throws IOException {
    String hadoopConfFileName = config.getString(hadoopConfigFileKey);
    if (hadoopConfFileName != null) {
      Configuration.addDefaultResource(hadoopConfFileName);
    }
    conf = new Configuration();

    super.initializeConfig(config);

    String rootDirStr = config.getString(rootDirsConfig);
    String[] rootDirStrs;
    if (rootDirStr == null) {
      throw new IllegalArgumentException("Missing root dir configuration:" 
          + rootDirsConfig);
    } else {
      rootDirStrs = rootDirStr.split(",");
    }
    rootDirs = new Path[rootDirStrs.length];
    clusterNames = new String[rootDirStrs.length];
    fileSystems = new FileSystem[rootDirStrs.length];
    String clusterName = "hadoopcluster";
    for (int i = 0; i < rootDirStrs.length; i++) {
      LOG.debug("Looking at rootDirStr:" + rootDirStrs[i]);
      rootDirs[i] = new Path(rootDirStrs[i]);
      fileSystems[i] = rootDirs[i].getFileSystem(conf);
      clusterNames[i] = clusterName + i;
    }

    inputFormatClassName = config.getString(inputFormatClassNameConfig,
        DEFAULT_INPUT_FORMAT_CLASSNAME);
  }

  protected void createPartitionReaders() throws IOException {
    for (int i= 0; i < clusterNames.length; i++) {
      String clusterName = clusterNames[i];
      LOG.debug("Creating partition reader for cluster:" + clusterName);
     
      // create partition id
      PartitionId id = new PartitionId(clusterName, null);
      Map<Integer, PartitionCheckpoint> listofPartitionCheckpoints = new 
      		TreeMap<Integer, PartitionCheckpoint>();

      PartitionCheckpointList partitionCheckpointList = new 
      		PartitionCheckpointList(listofPartitionCheckpoints);    
      ((CheckpointList)currentCheckpoint).preaprePartitionCheckPointList(id, 
      		partitionCheckpointList);   

      // calculate the allowed start time
      long currentMillis = System.currentTimeMillis();
      Date allowedStartTime = new Date(currentMillis - 
          (retentionInHours * ONE_HOUR_IN_MILLIS));
      Date partitionTimestamp = getPartitionTimestamp(id,
          currentCheckpoint, allowedStartTime);
      PartitionReaderStatsExposer clusterMetrics = 
      		new PartitionReaderStatsExposer(topicName, consumerName, id.toString(), 
          		consumerNumber);
      addStatsExposer(clusterMetrics);
      PartitionReader reader = new PartitionReader(id,
          partitionCheckpointList, fileSystems[i], buffer, rootDirs[i],
          conf, inputFormatClassName, partitionTimestamp,
          waitTimeForFileCreate, false, dataEncodingType, clusterMetrics, 
          partitionMinList);    
      LOG.debug("Created partition " + id);
      readers.put(id, reader);
    }
  }

  Configuration getHadoopConf() {
    return conf;
  }
  
  Path[] getRootDirs() {
    return rootDirs;
  }
  
  @Override
  protected void createCheckpoint() {
  	currentCheckpoint = new CheckpointList(partitionMinList); 
  }

}
