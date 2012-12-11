package com.inmobi.messaging.consumer.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.BaseMessageConsumerStatsExposer;
import com.inmobi.messaging.consumer.databus.AbstractMessagingDatabusConsumer;
import com.inmobi.messaging.consumer.util.ConsumerUtil;
import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.MessageUtil;

public class TestHadoopConsumerWithPartitionList  {
	protected static final Log LOG = LogFactory.getLog(
			TestHadoopConsumerWithPartitionList.class);
	HadoopConsumer consumer;
	HadoopConsumer secondConsumer;
	String firstConfFile = "messaging-consumer-hadoop-conf11.properties";
	String secondConfFile = "messaging-consumer-hadoop-conf12.properties";
	String streamName = "testStream";
	int numMessagesPerFile = 100;
  int numDataFiles;
  int numSuffixDirs;
  protected String[] dataFiles = new String[] {HadoopUtil.files[0],
      HadoopUtil.files[1],
      HadoopUtil.files[2], HadoopUtil.files[3]};
  protected String[] suffixDirs;
  protected String consumerName = "c1";
  protected Path[] rootDirs;
  Path [][] finalPaths;
  Configuration conf;
	ClientConfig firstConsumerConfig;
  ClientConfig secondConsuemrConfig;
  protected String ck8;
  
	boolean hadoop = true;
	
	@BeforeTest
	public void setup() throws Exception {
		firstConsumerConfig = ClientConfig.loadFromClasspath(firstConfFile);
		secondConsuemrConfig = ClientConfig.loadFromClasspath(secondConfFile);
	 
	  createFiles(consumer);
	  
	  ck8 = "/tmp/test/hadoop/8/checkpoint";
	  consumer = new HadoopConsumer();
	  secondConsumer = new HadoopConsumer();
	}
	
	public void createFiles(HadoopConsumer consumer) throws Exception {
  	consumer = new HadoopConsumer();
    consumer.initializeConfig(firstConsumerConfig);
    
    conf = consumer.getHadoopConf();
    Assert.assertEquals(conf.get("myhadoop.property"), "myvalue");
    
    rootDirs = consumer.getRootDirs();
    LOG.info("size is" + rootDirs.length);
    LOG.info("number of root dirs   "+ rootDirs.length);
    numSuffixDirs = suffixDirs != null ? suffixDirs.length : 1;
    numDataFiles = dataFiles != null ? dataFiles.length : 1;
    finalPaths = new Path[rootDirs.length][numSuffixDirs * numDataFiles];
    for (int i = 0; i < rootDirs.length; i++) {
      HadoopUtil.setupHadoopCluster(
          conf, dataFiles, suffixDirs, finalPaths[i], rootDirs[i]);
    }
    HadoopUtil.setUpHadoopFiles(rootDirs[0], conf, 
        new String[] {"_SUCCESS", "_DONE"}, suffixDirs, null);
	}
	
	@Test
	public void testConsumerMarkAndResetWithStartTime() throws Exception {
		firstConsumerConfig.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[1].toString());
		firstConsumerConfig.set(HadoopConsumerConfig.checkpointDirConfig,
        ck8);
		secondConsuemrConfig.set(HadoopConsumerConfig.rootDirsConfig, 
				rootDirs[1].toString());
		secondConsuemrConfig.set(HadoopConsumerConfig.checkpointDirConfig, ck8);
		ConsumerUtil.testConsumerMarkAndResetWithStartTime(firstConsumerConfig,
				secondConsuemrConfig, streamName, consumerName,
        DatabusStreamWaitingReader.getDateFromStreamDir(
            rootDirs[1], finalPaths[0][1]), true);  
	}
	
	@AfterTest
	public void cleanup() throws IOException {
    FileSystem lfs = FileSystem.getLocal(conf);
    for (Path rootDir : rootDirs) {
      lfs.delete(rootDir.getParent(), true);
    }
  }
}