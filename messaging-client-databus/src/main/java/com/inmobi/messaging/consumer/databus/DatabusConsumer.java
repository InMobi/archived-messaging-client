package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

/**
 * Consumes data from the configured databus stream topic. 
 * 
 * Initializes the databus configuration from the configuration file specified
 * by the configuration {@value DatabusConsumerConfig#databusConfigFileKey},
 * the default value is
 * {@value DatabusConsumerConfig#DEFAULT_DATABUS_CONFIG_FILE} 
 *
 * Consumer can specify a comma separated list of clusters from which the stream
 * should be streamed via configuration 
 * {@value DatabusConsumerConfig#databusClustersConfig}. If no
 * such configuration exists, it will stream from all the source clusters of the 
 * stream.
 *  
 * This consumer supports mark and reset. Whenever user calls mark, the current
 * consumption will be check-pointed in a directory configurable via 
 * {@value DatabusConsumerConfig#checkpointDirConfig}. The default value for
 * value for checkpoint
 * directory is {@value DatabusConsumerConfig#DEFAULT_CHECKPOINT_DIR}. After
 * reset(), consumer will start reading
 * messages from last check-pointed position.
 * 
 * Maximum consumer buffer size is configurable via 
 * {@value DatabusConsumerConfig#queueSizeConfig}. 
 * The default value is {@value DatabusConsumerConfig#DEFAULT_QUEUE_SIZE}.
 * 
 * If consumer is reading from the file that is currently being written by
 * producer, consumer will wait for flush to happen on the file. The wait time
 * for flush is configurable via 
 * {@value DatabusConsumerConfig#waitTimeForFlushConfig}, and default
 * value is {@value DatabusConsumerConfig#DEFAULT_WAIT_TIME_FOR_FLUSH}
 *
 * Initializes partition readers for each active collector on the stream.
 * TODO: Dynamically detect if new collectors are added and start readers for
 *  them 
 */
public class DatabusConsumer extends AbstractMessagingDatabusConsumer 
    implements DatabusConsumerConfig {
  private static final Log LOG = LogFactory.getLog(DatabusConsumer.class);

  private long waitTimeForFlush;
  private Path[] rootDirs;
  private StreamType streamType;
  private Configuration conf = new Configuration();
  private static String clusterNamePrefix = "databusCluster";

  protected void initializeConfig(ClientConfig config) throws IOException {
  	String type = config.getString(databusStreamType, DEFAULT_STREAM_TYPE);
    streamType = StreamType.valueOf(type);
    
   
    super.initializeConfig(config);
    waitTimeForFlush = config.getLong(waitTimeForFlushConfig,
        DEFAULT_WAIT_TIME_FOR_FLUSH);
    dataEncodingType = DataEncodingType.valueOf(
        config.getString(dataEncodingConfg, DataEncodingType.BASE64.name()));
    String rootDirsStr = config.getString(databusRootDirsConfig);
    String[] rootDirSplits;
    if (rootDirsStr != null) {
      rootDirSplits = rootDirsStr.split(",");
    } else {
      throw new IllegalArgumentException("Databus root directory not specified");
    }

    rootDirs = new Path[rootDirSplits.length];
    for (int i = 0; i < rootDirSplits.length; i++) {
      rootDirs[i] = new Path(rootDirSplits[i]);
    }
    if (streamType.equals(StreamType.MERGED)) {
      if (rootDirs.length > 1) {
        throw new IllegalArgumentException("Multiple directories are not" +
          " allowed for merge stream");
      }
    }
    LOG.info("Databus consumer initialized with streamName:" + topicName +
        " consumerName:" + consumerName + " startTime:" + startTime +
        " queueSize:" + bufferSize + " checkPoint:" + currentCheckpoint +
        " streamType:" + streamType);
  }

  private List<String> getCollectors(FileSystem fs, Path baseDir)
      throws IOException {
    List<String> collectors = new ArrayList<String>();    
    LOG.debug("Stream dir: " + baseDir);
    FileStatus[] list = fs.listStatus(baseDir);
    if (list != null && list.length > 0) {
      for (FileStatus status : list) {
        collectors.add(status.getPath().getName());
      }
    } else {
      LOG.warn("No collector dirs available in " + baseDir);
    }
    return collectors;
  }

  protected void createPartitionReaders() throws IOException {
    
    // calculate the allowed start time
    long currentMillis = System.currentTimeMillis();
    Date allowedStartTime = new Date(currentMillis - 
        (retentionInHours * ONE_HOUR_IN_MILLIS));

    for (int i = 0; i < rootDirs.length; i++) {
      LOG.debug("Creating partition readers for rootDir:" + rootDirs[i]);
      FileSystem fs = rootDirs[i].getFileSystem(conf);
      Path streamDir = DatabusUtil.getStreamDir(streamType, rootDirs[i],
          topicName);
      String clusterName = clusterNamePrefix + i;
      if (streamType.equals(StreamType.COLLECTOR)) {
      	Map<PartitionId, PartitionCheckpoint> partitionsChkPoints = 
      			 ((Checkpoint)currentCheckpoint).getPartitionsCheckpoint();
        LOG.info("Creating partition readers for all the collectors");
        for (String collector : getCollectors(fs, streamDir)) {
          PartitionId id = new PartitionId(clusterName, collector);
          if (partitionsChkPoints.get(id) == null) {
            partitionsChkPoints.put(id, null);
          }
          Date partitionTimestamp = getPartitionTimestamp(id,
          		partitionsChkPoints.get(id), allowedStartTime);
          LOG.debug("Creating partition " + id);
          PartitionReaderStatsExposer collectorMetrics = new 
              CollectorReaderStatsExposer(topicName, consumerName,
                  id.toString(), consumerNumber);
          addStatsExposer(collectorMetrics);
          readers.put(id, new PartitionReader(id,
              partitionsChkPoints.get(id), conf, fs,
              new Path(streamDir, collector), 
              DatabusUtil.getStreamDir(StreamType.LOCAL, rootDirs[i], topicName),
              buffer, topicName, partitionTimestamp,
              waitTimeForFlush, waitTimeForFileCreate, dataEncodingType,
              collectorMetrics));
        }
      } else {
        LOG.info("Creating partition reader for cluster");
        PartitionId id = new PartitionId(clusterName, null);
        Map<Integer, PartitionCheckpoint> listofPartitionCheckpoints = new 
        		TreeMap<Integer, PartitionCheckpoint>();
        PartitionCheckpointList partitionCheckpointList = new 
        		PartitionCheckpointList(listofPartitionCheckpoints);
        ((CheckpointList)currentCheckpoint).preaprePartitionCheckPointList(id, 
        		partitionCheckpointList);
        Date partitionTimestamp = getPartitionTimestamp(id,
            partitionCheckpointList, allowedStartTime);
        LOG.debug("Creating partition " + id);
        PartitionReaderStatsExposer clusterMetrics = 
            new PartitionReaderStatsExposer(topicName, consumerName,
                id.toString(), consumerNumber);
        addStatsExposer(clusterMetrics);
        readers.put(id, new PartitionReader(id,
            partitionCheckpointList, fs, buffer, streamDir, conf,
            TextInputFormat.class.getCanonicalName(), partitionTimestamp,
            waitTimeForFileCreate, true, dataEncodingType, clusterMetrics, 
            partitionMinList));
      }
    }
  }

  Path[] getRootDirs() {
    return rootDirs;
  }
  
  @Override
  protected void createCheckpoint() {
  	// TODO Auto-generated method stub
  	Map<PartitionId, PartitionCheckpoint> partitionCheckpoint = new 
  			HashMap<PartitionId, PartitionCheckpoint>();
  	if (streamType.equals(StreamType.COLLECTOR)) {
  		currentCheckpoint = new Checkpoint(partitionCheckpoint);
  	} else {
  		currentCheckpoint = new CheckpointList(partitionMinList);
  	}
  }
}
