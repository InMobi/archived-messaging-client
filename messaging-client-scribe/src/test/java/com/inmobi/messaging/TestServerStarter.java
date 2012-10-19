package com.inmobi.messaging;

import static org.testng.Assert.assertNotNull;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import random.pkg.NtMultiServer;

import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestServerStarter {
  private static NtMultiServer server;

  @BeforeSuite
  public void setUp() {
    safeInit();
  }

  @AfterSuite
  public void tearDown() {
    server.stop();
  }

  public static NtMultiServer getServer() {
    safeInit();
    assertNotNull(server);
    return server;
  }

  private static synchronized void safeInit() {
    if (server == null) {
      server = new NtMultiServer();
    }
  }

  public static final int port = 7912;

  public static ScribeMessagePublisher createPublisher(int port, 
      int timeout) throws Exception {
    return createPublisher(port, timeout, 5);
  }

  public static ScribeMessagePublisher createPublisher(int port, 
      int timeout, int backOff) throws Exception {
    return createPublisher(port, timeout, backOff, true, true);
  }

  public static ScribeMessagePublisher createPublisher(int port, 
      int timeout, int backOff, boolean enableRetries, boolean resendOnAckLost)
          throws Exception {
    return createPublisher(port, timeout, backOff, enableRetries,
        resendOnAckLost, 100, 100);
  }

  public static ScribeMessagePublisher createPublisher(int port, 
      int timeout, int backOff, boolean enableRetries, boolean resendOnAckLost,
      int msgQueueSize, int ackQueueSize)
          throws Exception {
    return createPublisher(port, timeout, backOff, enableRetries,
        resendOnAckLost, msgQueueSize, ackQueueSize, -1);
  }

  public static ScribeMessagePublisher createPublisher(int port, 
      int timeout, int backOff, boolean enableRetries, boolean resendOnAckLost,
      int msgQueueSize, int ackQueueSize, int numRetries)
          throws Exception {
    ScribeMessagePublisher pub = new ScribeMessagePublisher();
    pub.init("localhost", port, backOff, timeout, enableRetries,
        resendOnAckLost, 10, msgQueueSize, ackQueueSize, numRetries);
    return pub;
  }

  public static ScribeMessagePublisher createPublisher() throws Exception {
    return createPublisher(port, 5);
  }
}
