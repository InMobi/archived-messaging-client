package com.inmobi.messaging.consumer.hadoop;

import java.io.IOException;
import java.util.ArrayList;
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

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.hadoop.HadoopConsumer;

public class TestConsumerPartitionRetention {
	protected static final Log LOG = LogFactory.getLog(
			TestConsumerPartitionRetention.class);
	HadoopConsumer consumer;
	HadoopConsumer secondConsumer;
	String firstConfFile = "messaging-consumer-hadoop-conf7.properties";
	String secondConfFile = "messaging-consumer-hadoop-conf8.properties";
	String streamName = "testStream";
	List<String> firstConsumedMessages;
	List<String> secondConsumedMessages;
	ClientConfig config;
	ClientConfig secondconfig;
	int numMessagesPerFile = 100;
  int numDataFiles;
  int numSuffixDirs;
  protected String[] dataFiles = new String[] {HadoopUtil.files[0],
      HadoopUtil.files[1], HadoopUtil.files[2], HadoopUtil.files[3]};
  protected String[] suffixDirs;
  protected String consumerName;
  protected Path[] rootDirs;
  Path [][] finalPaths;
  Configuration conf;
	
  @BeforeTest
	public void setup() throws Exception {
  	config = ClientConfig.loadFromClasspath(firstConfFile);
	  secondconfig = ClientConfig.loadFromClasspath(secondConfFile);
	  firstConsumedMessages = new ArrayList<String>();
	  secondConsumedMessages = new ArrayList<String>();
	  createFiles(consumer);
		
	  consumer = new HadoopConsumer();
	  secondConsumer = new HadoopConsumer();
  }
  
  public void createFiles(HadoopConsumer consumer) throws Exception {
  	consumer = new HadoopConsumer();
    consumer.initializeConfig(config);
    conf = consumer.getHadoopConf();
    rootDirs = consumer.getRootDirs();
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
	public void testWithRetentionPeriod() throws Exception {
 
		consumer.init(streamName, consumerName, null, config);
		LOG.info("topicname is" + streamName + consumer.getTopicName());
		Assert.assertEquals(consumer.getTopicName(), streamName);
		Assert.assertEquals(consumer.getConsumerName(), consumerName);
	  
		secondConsumer.init(streamName, consumerName, null, secondconfig);
		Assert.assertEquals(secondConsumer.getTopicName(), streamName);
		Assert.assertEquals(secondConsumer.getConsumerName(), consumerName);
		LOG.info("checking: whether consumers started at same time" + 
				consumer.getStartTime() + "   " + secondConsumer.getStartTime());
	  Assert.assertEquals(secondConsumer.getStartTime(), consumer.getStartTime());
	
		//consume all messages
		while (firstConsumedMessages.size() < 600) {
		  Message msg = consumer.next();
		  firstConsumedMessages.add(getMessage(msg.getData().array()));
	  }
		consumer.close();
    LOG.info("number of msgs consumed by first consumer" + 
    		firstConsumedMessages.size());
	  
	  while (secondConsumedMessages.size() < 600) {
	  	Message msgs = secondConsumer.next();
	  	secondConsumedMessages.add(getMessage(msgs.getData().array()));
	  }
  	secondConsumer.close();
	  LOG.info("number of messages consumed messages by second consumer " + 
	  		secondConsumedMessages.size());
	  
		Assert.assertEquals(firstConsumedMessages.size() + 
				secondConsumedMessages.size(), 1200);
	}
  
  @AfterTest
  public void cleanup() throws Exception {
  	FileSystem lfs = FileSystem.getLocal(conf);
  	for (Path rootDir : rootDirs) {
  		lfs.delete(rootDir.getParent(), true);
  	}
  }

  private static String getMessage(byte[] array) throws IOException {
 		return MessageUtil.getTextMessage(array).toString();
 	}
}
