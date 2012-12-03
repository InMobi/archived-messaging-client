package com.inmobi.messaging.consumer.databus;

import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.Message;

public class QueueEntry {

  private final Message message;
  private final PartitionId partitionId;
  private final MessageCheckpoint msgCheckpoint;

  public QueueEntry(Message msg, PartitionId partitionId,
  	 	MessageCheckpoint msgCheckpoint) {
    this.message = msg;
    this.partitionId = partitionId;
    this.msgCheckpoint = msgCheckpoint;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public MessageCheckpoint getMessageChkpoint() {
  	return msgCheckpoint;
  }

  public Message getMessage() {
    return message;
  }
}
