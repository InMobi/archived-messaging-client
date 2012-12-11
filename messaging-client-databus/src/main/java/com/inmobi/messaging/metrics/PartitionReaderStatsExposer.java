package com.inmobi.messaging.metrics;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionReaderStatsExposer extends 
    DatabusConsumerStatsExposer {
  public final static String MESSAGES_READ_FROM_SOURCE = 
      "messagesReadFromSource";
  public final static String MESSAGES_ADDED_TO_BUFFER = "messagesAddedToBuffer";
  public final static String HANDLED_EXCEPTIONS = "handledExceptions";
  public final static String WAIT_TIME_UNITS_NEW_FILE = "waitTimeUnitsNewFile";
  public final static String PARTITION_CONTEXT = "PartitionId";
  public final static String CUMULATIVE_NANOS_FETCH_MESSAGE = 
      "cumulativeNanosForFecthMessage";

  private final AtomicLong numMessagesReadFromSource = new AtomicLong(0);
  private final AtomicLong numMessagesAddedToBuffer = new AtomicLong(0);
  private final AtomicLong numHandledExceptions = new AtomicLong(0);
  private final AtomicLong numWaitTimeUnitsNewFile = new AtomicLong(0);
  private final AtomicLong cumulativeNanosForFecthMessage = new AtomicLong(0);
  private final String pid;

  public PartitionReaderStatsExposer(String topicName, String consumerName,
      String pid, int consumerNumber) {
    super(topicName, consumerName, consumerNumber);
    this.pid = pid;
  }

  public void incrementMessagesReadFromSource() {
    numMessagesReadFromSource.incrementAndGet();
  }

  public void incrementMessagesAddedToBuffer() {
    numMessagesAddedToBuffer.incrementAndGet();
  }

  public void incrementHandledExceptions() {
    numHandledExceptions.incrementAndGet();
  }

  public void incrementWaitTimeUnitsNewFile() {
    numWaitTimeUnitsNewFile.incrementAndGet();
  }

  public void addCumulativeNanosFetchMessage(long nanos) {
    cumulativeNanosForFecthMessage.addAndGet(nanos);
  }

  @Override
  protected void addToStatsMap(Map<String, Number> map) {
    map.put(MESSAGES_READ_FROM_SOURCE, getMessagesReadFromSource());
    map.put(MESSAGES_ADDED_TO_BUFFER, getMessagesAddedToBuffer());
    map.put(HANDLED_EXCEPTIONS, getHandledExceptions());
    map.put(WAIT_TIME_UNITS_NEW_FILE, getWaitTimeUnitsNewFile());
    map.put(CUMULATIVE_NANOS_FETCH_MESSAGE, getCumulativeNanosForFetchMessage());
  }

  @Override
  protected void addToContextsMap(Map<String, String> map) {
    super.addToContextsMap(map);
    map.put(PARTITION_CONTEXT, pid);
  }

  public long getMessagesReadFromSource() {
    return numMessagesReadFromSource.get();
  }

  public long getMessagesAddedToBuffer() {
    return numMessagesAddedToBuffer.get();
  }

  public long getHandledExceptions() {
    return numHandledExceptions.get();
  }

  public long getWaitTimeUnitsNewFile() {
    return numWaitTimeUnitsNewFile.get();
  }

  public long getCumulativeNanosForFetchMessage() {
    return cumulativeNanosForFecthMessage.get();
  }
}
