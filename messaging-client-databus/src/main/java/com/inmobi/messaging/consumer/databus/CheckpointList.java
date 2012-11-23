package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;

/**
 * Checkpoint for the segments of databus stream consumer. 
 * 
 */
public class CheckpointList implements ConsumerCheckpoint {

  // map of static id to its checkpoint
  private Map<Integer, Checkpoint> chkpoints =
      new TreeMap<Integer, Checkpoint>();
  private final List<Integer> idList;

  public CheckpointList(List<Integer> idList,
      CheckpointProvider provider, String superKey) {
    this.idList = idList;
  }

  void setCheckpoint(Map<Integer, Checkpoint> chkpoints) {
    this.chkpoints = chkpoints;
  }

  public  Map<Integer, Checkpoint> getCheckpoints() {
    return chkpoints;
  }

  public void set(PartitionId pid, MessageCheckpoint msgCkp) {
    PartitionCheckpointList pckList = (PartitionCheckpointList) msgCkp;
    for (Map.Entry<Integer, PartitionCheckpoint> entry : pckList.
        getCheckpoints().entrySet()) {
      Checkpoint cp = chkpoints.get(entry.getKey());
      if (cp == null) {
        Map<PartitionId, PartitionCheckpoint> partitionsChkPoints = 
            new HashMap<PartitionId, PartitionCheckpoint>();
        cp = new Checkpoint(partitionsChkPoints);
      }
      cp.set(pid, entry.getValue());
    }
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    for (Map.Entry<Integer, Checkpoint> entry : chkpoints
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

  public static String getChkpointKey(String superKey, int id) {
    return superKey + "_" + id;
  }

  public void write(CheckpointProvider checkpointProvider, String superKey)
      throws IOException {
    for (Map.Entry<Integer, Checkpoint> entry : chkpoints.entrySet()) {
      checkpointProvider.checkpoint(getChkpointKey(superKey, entry.getKey()),
          entry.getValue().toBytes());
    }
  }

  public void read(CheckpointProvider checkpointProvider, String superKey)
      throws IOException {
    Map<Integer, Checkpoint> thisChkpoint = new TreeMap<Integer, Checkpoint>();
    for (Integer id : idList) {
      byte[] chkpointData = checkpointProvider.read(
          getChkpointKey(superKey, id));
      Checkpoint checkpoint;
      if (chkpointData != null) {
        checkpoint = new Checkpoint(chkpointData);
      } else {
        Map<PartitionId, PartitionCheckpoint> partitionsChkPoints = 
            new HashMap<PartitionId, PartitionCheckpoint>();
        checkpoint = new Checkpoint(partitionsChkPoints);
      }
      thisChkpoint.put(id, checkpoint);
    }
    setCheckpoint(thisChkpoint);
  }
}
