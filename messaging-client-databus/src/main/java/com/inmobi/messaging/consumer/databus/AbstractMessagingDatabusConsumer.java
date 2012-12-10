package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.utils.SecureLoginUtil;
import com.inmobi.instrumentation.AbstractMessagingClientStatsExposer;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;
import com.inmobi.messaging.metrics.DatabusConsumerStatsExposer;

public abstract class AbstractMessagingDatabusConsumer 
    extends AbstractMessageConsumer 
    implements MessagingConsumerConfig {
  protected static final Log LOG = LogFactory.getLog(
      AbstractMessagingDatabusConsumer.class);
  protected static final long ONE_HOUR_IN_MILLIS = 1 * 60 * 60 * 1000;

  protected BlockingQueue<QueueEntry> buffer;

  protected final Map<PartitionId, PartitionReader> readers = 
      new HashMap<PartitionId, PartitionReader>();

  protected CheckpointProvider checkpointProvider;
  protected ConsumerCheckpoint currentCheckpoint;
  protected long waitTimeForFileCreate;
  protected int bufferSize;
  protected DataEncodingType dataEncodingType;
  protected int retentionInHours;
  protected int consumerNumber;
  protected int totalConsumers;
  public Set<Integer> partitionMinList;
  @Override
  protected void init(ClientConfig config) throws IOException {
    initializeConfig(config);
    start();
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

  protected void initializeConfig(ClientConfig config) throws IOException {
    super.init(config);
    // verify authentication
    if (UserGroupInformation.isSecurityEnabled()) {
      String principal = config.getString(consumerPrincipal);
      String keytab = config.getString(consumerKeytab);
      if (principal != null && keytab != null) {
        SecureLoginUtil.login(consumerPrincipal, principal,
            consumerKeytab, keytab);
      } else {
        LOG.info("There is no principal or key tab file passed. Using the" +
            " commandline authentication.");
      }
    }

    // Read consumer id
    String consumerIdStr = config.getString(consumerIdInGroupConfig,
        DEFAULT_CONSUMER_ID);
    String[] id = consumerIdStr.split("/");
    try {
      consumerNumber = Integer.parseInt(id[0]);
      totalConsumers = Integer.parseInt(id[1]);
      partitionMinList = new TreeSet<Integer>();
      if (consumerNumber > 0 && totalConsumers > 0) {
      	for (int i = 0; i < 60; i++) {
      		if ((i % totalConsumers) == (consumerNumber - 1)) {
      			partitionMinList.add(i);
      		}
      	}
      } else {
      	LOG.info("Invalid consumer group membership");
      }
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Invalid consumer group membership",
          nfe);
    }
    // Create checkpoint provider and initialize checkpoint
    String chkpointProviderClassName = config.getString(
        chkProviderConfig, DEFAULT_CHK_PROVIDER);
    String databusCheckpointDir = config.getString(checkpointDirConfig, 
        DEFAULT_CHECKPOINT_DIR);
    this.checkpointProvider = createCheckpointProvider(
        chkpointProviderClassName, databusCheckpointDir);

    createCheckpoint();
    currentCheckpoint.read(checkpointProvider, getChkpointKey());

    //create buffer
    bufferSize = config.getInteger(queueSizeConfig, DEFAULT_QUEUE_SIZE);
    buffer = new LinkedBlockingQueue<QueueEntry>(bufferSize);

    // initialize other common configuration
    waitTimeForFileCreate = config.getLong(waitTimeForFileCreateConfig,
        DEFAULT_WAIT_TIME_FOR_FILE_CREATE);
    dataEncodingType = DataEncodingType.valueOf(
        config.getString(dataEncodingConfg, DEFAULT_DATA_ENCODING));

    // get the retention period of the topic
    retentionInHours = config.getInteger(retentionConfig,
        DEFAULT_RETENTION_HOURS); 

    LOG.debug("Using data encoding type as " + dataEncodingType);
  }

  public Map<PartitionId, PartitionReader> getPartitionReaders() {
    return readers;
  }

  protected abstract void createCheckpoint();

  public Set<Integer> getPartitionMinList() {
  	return partitionMinList;
  }

  public ConsumerCheckpoint getCurrentCheckpoint() {
  	return currentCheckpoint;
  }

  @Override
  protected Message getNext() throws InterruptedException {
    QueueEntry entry;
    entry = buffer.take();
    MessageCheckpoint msgchk = entry.getMessageChkpoint();
    currentCheckpoint.set(entry.getPartitionId(), msgchk);
    return entry.getMessage();
  }

  protected synchronized void start() throws IOException {
    createPartitionReaders();
    for (PartitionReader reader : readers.values()) {
      reader.start();
    }
  }

  protected abstract void createPartitionReaders() throws IOException;

  protected Date getPartitionTimestamp(PartitionId id, PartitionCheckpoint pck,
      Date allowedStartTime) {
    Date partitionTimestamp = startTime;
    if (startTime == null && pck == null) {
      LOG.info("There is no startTime passed and no checkpoint exists" +
          " for the partition: " + id + " starting from the start" +
          " of the stream.");
      partitionTimestamp = allowedStartTime;
    } else if (startTime != null && startTime.before(allowedStartTime)) {
      LOG.info("Start time passed is before the start of the stream," +
          " starting from the start of the stream.");
      partitionTimestamp = allowedStartTime;
    } else {
      LOG.info("Creating partition with timestamp: " + partitionTimestamp
          + " checkpoint:" + pck);
    }
    return partitionTimestamp;
  }
  
  protected Date getPartitionTimestamp(PartitionId id, PartitionCheckpointList pck,
    	Date allowedStartTime) {
  	boolean checkpointFlag = isPartitionCheckpointListNUll(pck);
  	Date partitionTimestamp = startTime;
  	if (startTime == null && !checkpointFlag) {
  		LOG.info("There is no startTime passed and no checkpoint exists" +
  				" for the partition: " + id + " starting from the start" +
  				" of the stream.");
  		partitionTimestamp = allowedStartTime;
  	} else if (startTime != null && startTime.before(allowedStartTime)) {
  		LOG.info("Start time passed is before the start of the stream," +
  				" starting from the start of the stream.");
  		partitionTimestamp = allowedStartTime;
  	} else {
  		LOG.info("Creating partition with timestamp: " + partitionTimestamp
  				+ " checkpoint:" + pck);
  	}

  	return partitionTimestamp;
  }

  public boolean isPartitionCheckpointListNUll(PartitionCheckpointList 
  		partitionCheckpointList) {
  	Map<Integer, PartitionCheckpoint>listOfPartitionCheckpoints = 
  			partitionCheckpointList.getCheckpoints();
  	for (Integer minuteId : partitionMinList) {
  		if ( listOfPartitionCheckpoints.get(minuteId)!= null) {
  			return true;
  		}
  	}
  	return false;
  }
  

  protected String getChkpointKey() {
    return consumerName + "_" + topicName;
  }

  @Override
  protected void doReset() throws IOException {
    // restart the service, consumer will start streaming from the last saved
    // checkpoint
    close();
    currentCheckpoint.read(checkpointProvider, getChkpointKey());
    LOG.info("Resetting to checkpoint:" + currentCheckpoint);
    // reset to last marked position, ignore start time
    startTime = null;
    buffer = new LinkedBlockingQueue<QueueEntry>(bufferSize);
    start();
  }

  @Override
  protected void doMark() throws IOException {
  	currentCheckpoint.write(checkpointProvider, getChkpointKey());
    LOG.info("Committed checkpoint:" + currentCheckpoint);
  }

  @Override
  public synchronized void close() {
    for (PartitionReader reader : readers.values()) {
      reader.close();
      removeStatsExposer(reader.getStatsExposer());
    }
    readers.clear();
    buffer.clear();
    super.close();
  }

  @Override
  public boolean isMarkSupported() {
    return true;
  }

  @Override
  protected AbstractMessagingClientStatsExposer getMetricsImpl() {
    return new DatabusConsumerStatsExposer(topicName, consumerName,
        consumerNumber);
  }
}
