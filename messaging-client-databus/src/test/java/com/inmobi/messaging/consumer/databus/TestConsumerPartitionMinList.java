package com.inmobi.messaging.consumer.databus;

import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;

public class TestConsumerPartitionMinList {
	
	protected static final Log LOG = LogFactory.getLog(
      TestConsumerPartitionMinList.class);
	int consumerId = 1;
	int totalNumberOfConsumers = 2;
	Set<Integer> expectedPartitionMinList;
	DatabusConsumer testConsumer;
    
	@BeforeTest
	public void setup() throws Exception {
		ClientConfig config = ClientConfig.loadFromClasspath(
        "messaging-consumer-conf4.properties");
		
		expectedPartitionMinList = new TreeSet<Integer>();
		testConsumer = new DatabusConsumer();
    testConsumer.initializeConfig(config);
    if (totalNumberOfConsumers > 0 && consumerId > 0) {
    	expectedPartitionMinList();
    }
	}
	
	public void expectedPartitionMinList() throws Exception {
		for (int i = 0; i < 60; i++ ) {
			if ((i % totalNumberOfConsumers) == (consumerId - 1)) {   
				expectedPartitionMinList.add(i);
			}
		}
	}
	
	@Test
	public void testPartitionMinList() {
		Set<Integer> actualPartitionMinList = testConsumer.getPartitionMinList();
		Assert.assertEquals(consumerId, testConsumer.consumerNumber);
		Assert.assertEquals(totalNumberOfConsumers, testConsumer.totalConsumers);
		Assert.assertEquals(expectedPartitionMinList.size(), 
				actualPartitionMinList.size());
		expectedPartitionMinList.containsAll(actualPartitionMinList);
	}
	
	@AfterTest
	public void cleanUp() {
		testConsumer.close();
	}
	
}
