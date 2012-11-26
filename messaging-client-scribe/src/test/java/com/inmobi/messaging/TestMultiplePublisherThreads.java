package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;

import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestMultiplePublisherThreads {
  private NtMultiServer server;
  private ScribeMessagePublisher publisher; 
  
  @BeforeTest
  public void setUp() throws Exception {
    server = TestServerStarter.getServer();
    server.start();
    publisher = TestServerStarter.createPublisher();
  }

  @AfterTest
  public void tearDown() {
    server.stop();
    if (publisher != null)
      publisher.close();
  }
  
  @Test
  public void multipleThreadSinglePublisher() throws Exception{
    String topic = "test";
    CountDownLatch startLatch = new CountDownLatch(10);
    PublishThread p1 = new PublishThread(startLatch, topic, publisher);
    PublishThread p2 = new PublishThread(startLatch, topic, publisher);
    PublishThread p3 = new PublishThread(startLatch, topic, publisher);
    PublishThread p4 = new PublishThread(startLatch, topic, publisher);
    p1.start();
    p2.start();
    p3.start();
    p4.start();
    Thread.sleep(3000);
    p1.join();
    p2.join();
    p3.join();
    p4.join();
    System.out.println(publisher.getStats(topic));
    assertEquals(publisher.getStats(topic).getSuccessCount(), 4);
    assertEquals(publisher.getStats(topic).getRetryCount(),0);
    assertEquals(publisher.getStats(topic).getReconnectionCount(),0);
    System.out.println("Test multipleThreadSinglePublisher is done");    
  }
  
  private void doTest(String topic, ScribeMessagePublisher publisher) {
    publisher.publish(topic, new Message("msg1".getBytes()));
    while (publisher.getStats(topic).getInFlight() != 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  class PublishThread extends Thread {
    
    private String topic;
    private ScribeMessagePublisher publisher;
    private final CountDownLatch startLatch;
      
    PublishThread(CountDownLatch startLatch, String topic, ScribeMessagePublisher publisher){
      this.startLatch= startLatch;
      this.topic = topic;
      this.publisher = publisher;
    }
      
    public void run(){
        doTest(topic, publisher);
        publisher.close();
    }
  }
}
