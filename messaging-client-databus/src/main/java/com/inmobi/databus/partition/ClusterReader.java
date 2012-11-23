package com.inmobi.databus.partition;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.MessageCheckpoint;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class ClusterReader extends AbstractPartitionStreamReader {

  private static final Log LOG = LogFactory.getLog(PartitionReader.class);

  private final PartitionCheckpoint partitionCheckpoint;
  private final Date startTime;
  private final Path streamDir;
  private final boolean isDatabusData;

  ClusterReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, FileSystem fs,
      Path streamDir, Configuration conf, String inputFormatClass,
      Date startTime, long waitTimeForFileCreate, boolean isDatabusData,
      PartitionReaderStatsExposer metrics, boolean noNewFiles)
          throws IOException {
    this.startTime = startTime;
    this.streamDir = streamDir;
    this.partitionCheckpoint = partitionCheckpoint;
    this.isDatabusData = isDatabusData;

    reader = new DatabusStreamWaitingReader(partitionId, fs, streamDir,
        inputFormatClass, conf, waitTimeForFileCreate, metrics, noNewFiles);
  }

  public void initializeCurrentFile() throws IOException, InterruptedException {
    LOG.info("Initializing partition reader's current file");
    if (startTime != null) {
      ((DatabusStreamWaitingReader)reader).build(startTime);
      if (!reader.initializeCurrentFile(startTime)) {
        LOG.debug("Did not find the file associated with timestamp");
        reader.startFromTimestmp(startTime);
      }
    } else if (partitionCheckpoint != null) {
      ((DatabusStreamWaitingReader)reader).build(
          DatabusStreamWaitingReader.getBuildTimestamp(streamDir,
          partitionCheckpoint));
      if (!reader.isEmpty()) {
        if (!reader.initializeCurrentFile(partitionCheckpoint)) {
          throw new IllegalArgumentException("Checkpoint file does not exist");
        }
      } else {
        reader.startFromBegining();
      }
    } else {
      LOG.info("Would never reach here");
    }
    LOG.info("Intialized currentFile:" + reader.getCurrentFile() +
        " currentLineNum:" + reader.getCurrentLineNum());
  }

  public byte[] readLine() throws IOException, InterruptedException {
    byte[] line = super.readLine();
    if (line != null && isDatabusData) {
      Text text = new Text();
      ByteArrayInputStream bais = new ByteArrayInputStream(line);
      text.readFields(new DataInputStream(bais));
      return text.getBytes();
    } 
    return line;
  }

  @Override
  public MessageCheckpoint getMessageCheckpoint() {
    return msgChkpoint;
  }
}
