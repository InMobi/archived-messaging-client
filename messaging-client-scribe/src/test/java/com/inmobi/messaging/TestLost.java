package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import org.testng.Assert;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeAlwaysSuccess;
import random.pkg.ScribeAlwaysTryAgain;
import random.pkg.ScribeSlacker;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestLost {
  @Test()
  public void testMsgQueueSize() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7945;
      tserver = new NtMultiServer(new ScribeAlwaysSuccess(), port);

      int timeoutSeconds = 2;
      // create publisher with msg queue size 1
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, true, 1, 10);

      String topic = "retry";
      // publish two messages
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.publish(topic, new Message("mmmm".getBytes()));
      TimingAccumulator inspector = mb.getStats(topic);
      assertEquals(inspector.getLostCount(), 1,
          "Lost not incremented");
      tserver.start();
      while (inspector.getInFlight() != 0) {
        Thread.sleep(10);
      }
      mb.close();
      System.out.println("stats:" + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getLostCount(), 1,
          "Lost not incremented");
      assertEquals(inspector.getSuccessCount(), 1,
          "success not incremented");
    } finally {
      tserver.stop();
    }
  }

  @Test()
  public void testAckQueueSize() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7946;
      tserver = new NtMultiServer(new ScribeSlacker(), port);
      tserver.start();

      int timeoutSeconds = 2;
      // create publisher with msgqueue size 1
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, true, 1, 1);

      String topic = "retry";
      // publish 3 messages
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.publish(topic, new Message("mmmm".getBytes()));
      TimingAccumulator inspector = mb.getStats(topic);
      assertEquals(inspector.getLostCount(), 1,
          "Lost not incremented");
      while (inspector.getInFlight() != 0) {
        Thread.sleep(10);
      }
      mb.close();
      System.out.println("stats:" + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getLostCount(), 1,
          "Lost not incremented");
      assertEquals(inspector.getSuccessCount(), 2,
          "success not incremented");
    } finally {
      tserver.stop();
    }
  }

  @Test()
  public void testMsgQueueSizeOnRetries() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7947;
      tserver = new NtMultiServer(new ScribeAlwaysTryAgain(), port);
      tserver.start();

      int timeoutSeconds = 2;
      // create publisher with msgqueue size 1
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, true, 1, 1, 10);

      String topic = "retry";
      // publish 3 messages
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.publish(topic, new Message("mmmm".getBytes()));
      TimingAccumulator inspector = mb.getStats(topic);
      Assert.assertTrue(inspector.getLostCount() >= 1,
          "Wrong lost count");
      mb.close();
      System.out.println("stats:" + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getLostCount(), 3,
          "Lost not incremented");
    } finally {
      tserver.stop();
    }
  }
}
