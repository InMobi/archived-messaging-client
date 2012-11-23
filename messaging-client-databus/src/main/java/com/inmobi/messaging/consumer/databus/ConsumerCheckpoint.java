package com.inmobi.messaging.consumer.databus;

import java.io.IOException;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.partition.PartitionId;

public interface ConsumerCheckpoint {
  public void set(PartitionId pid, MessageCheckpoint pckList);
  
  public void read(CheckpointProvider checkpointProvider, String key)
      throws IOException;  

  public void write(CheckpointProvider checkpointProvider, String key)
      throws IOException;
}
