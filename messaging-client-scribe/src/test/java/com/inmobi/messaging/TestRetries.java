package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeAlternateTryLater;
import random.pkg.ScribeAlwaysTryAgain;
import random.pkg.ScribeSlacker;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestRetries {
  private NtMultiServer server;
  private ScribeMessagePublisher mb;

  @BeforeTest
  public void setUp() {
    server = TestServerStarter.getServer();
  }

  @AfterTest
  public void tearDown() {
    server.stop();
    if (mb != null)
      mb.close();
  }

  //@Test
  public void simpleSend() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7901;
      tserver = new NtMultiServer(new ScribeAlternateTryLater(), port);
      tserver.start();

      int timeoutSeconds = 10;
      mb = TestServerStarter.createPublisher(port, timeoutSeconds);

      String topic = "retry";
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.close();
      TimingAccumulator inspector = mb.getStats(topic);
      System.out.println("stats:" + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getRetryCount(), 1,
          "Retry not incremented");
      assertEquals(inspector.getSuccessCount(), 1,
          "success not incremented");
    } finally {
      tserver.stop();
    }
  }

  //@Test
  public void testEnableRetries() throws Exception {
    testEnableRetries(true);
    testEnableRetries(false);
  }

  public void testEnableRetries(boolean enableRetries) throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7902;
      tserver = new NtMultiServer(new ScribeAlternateTryLater(), port);
      tserver.start();

      int timeoutSeconds = 10;
      mb = TestServerStarter.createPublisher(port, timeoutSeconds, 1,
          enableRetries, true);

      String topic = "retry";
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.close();
      TimingAccumulator inspector = mb.getStats(topic);
      System.out.println("stats:" + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      if (enableRetries) {
        assertEquals(inspector.getRetryCount(), 1,
            "Retry not incremented");
        assertEquals(inspector.getSuccessCount(), 1,
            "success not incremented");
      } else {
        assertEquals(inspector.getGracefulTerminates(), 1,
            "Graceful terminates not incremented");
        assertEquals(inspector.getSuccessCount(), 0,
            "success incremented");
      }
    } finally {
      tserver.stop();
    }
  }

  //@Test
  public void testAlwaysTryAgain() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7903;
      tserver = new NtMultiServer(new ScribeAlwaysTryAgain(), port);
      tserver.start();

      int timeoutSeconds = 10;
      mb = TestServerStarter.createPublisher(port, timeoutSeconds, 1, true,
          true, 100, 100, 10);

      String topic = "retry";
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.close();
      TimingAccumulator inspector = mb.getStats(topic);
      System.out.println("stats:" + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      Assert.assertTrue(inspector.getRetryCount() > 0,
          "Retry not incremented");
      assertEquals(inspector.getLostCount(), 1,
          "Lost not incremented");
      assertEquals(inspector.getSuccessCount(), 0,
          "success incremented");
    } finally {
      tserver.stop();
    }
  }

  @Test
  public void testResendOnAckLost() throws Exception {
    testResendOnAckLost(true);
    testResendOnAckLost(false);
  }

  public void testResendOnAckLost(boolean resendOnAckLost) throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7904;
      tserver = new NtMultiServer(new ScribeSlacker(), port);
      tserver.start();

      int timeoutSeconds = 20;
      mb = TestServerStarter.createPublisher(port, timeoutSeconds, 1,
          true, resendOnAckLost);

      String topic = "retry";
      // publish a message and stop the server
      // ack will be lost for the above message. then start the server again
      mb.publish(topic, new Message("mmmm".getBytes()));
      Thread.sleep(500);
      tserver.stop();
      tserver.start();
      mb.close();
      TimingAccumulator inspector = mb.getStats(topic);
      System.out.println("stats:" + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      if (resendOnAckLost) {
        assertEquals(inspector.getSuccessCount(), 1,
            "success not incremented");
      } else {
        assertEquals(inspector.getGracefulTerminates(), 1,
            "Graceful terminates not incremented");
      }
    } finally {
      tserver.stop();
    }
  }

}
