package com.inmobi.databus.partition;

import java.util.Map;
import java.util.TreeMap;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.messaging.consumer.databus.MessageCheckpoint;

/**
 * Checkpoint for the segments of databus stream consumer. 
 * 
 */
public class PartitionCheckpointList implements MessageCheckpoint {

  // map of static id to its checkpoint
  private Map<Integer, PartitionCheckpoint> pChkpoints =
      new TreeMap<Integer, PartitionCheckpoint>();

  public PartitionCheckpointList(Map<Integer, PartitionCheckpoint> chkpoints) {
    this.pChkpoints = chkpoints;
  }

  public void setCheckpoint(Map<Integer, PartitionCheckpoint> chkpoints) {
    this.pChkpoints = chkpoints;
  }

  public Map<Integer, PartitionCheckpoint> getCheckpoints() {
    return pChkpoints;
  }

  public void set(int segmentId, PartitionCheckpoint pck) {
    pChkpoints.put(segmentId, pck);
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    for (Map.Entry<Integer, PartitionCheckpoint> entry : pChkpoints
        .entrySet()) {
      buf.append(entry.getKey().toString())
      .append(":");
      if (entry.getValue() != null) {
        buf.append(entry.getValue().toString());
      } else {
        buf.append("null");
      }
      buf.append(", ");
    }
    return buf.toString();
  }
}
