package com.inmobi.messaging.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import scribe.thrift.ResultCode;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.Message;

public class ScribeTopicPublisher {
  private static final Log LOG = LogFactory.getLog(ScribeTopicPublisher.class);

  private final Timer timer = new HashedWheelTimer();

  private ClientBootstrap bootstrap;
  private volatile Channel thisChannel = null;
  private String topic;
  private String host;
  private int port;
  private TimingAccumulator stats;
  private BlockingQueue<Message> toBeSent;
  private BlockingQueue<Message> toBeAcked;
  private long sleepInterval = 10;
  private boolean stopped = false;
  private Thread senderThread;
  private ScribeHandler handler;
  private boolean resendOnAckLost = false;
  private boolean reconnectionInProgress = false;
  private boolean enabledRetries = true;
  private int numDrainsOnClose = 10;

  /**
   * This is meant to be a way for async callbacks to set the channel on a
   * successful connection
   * 
   * Java does not have pointers to pointers. So have to resort to sending in a
   * wrapper object that knows to update our pointer
   */
  class ChannelSetter {
    public Channel getCurrentChannel() {
      return ScribeTopicPublisher.this.thisChannel;
    }

    public void setChannel(Channel ch) {
      Channel oldChannel = ScribeTopicPublisher.this.thisChannel;
      if (ch != oldChannel) {
        LOG.info("setting channel to " + ch.getId());
        ScribeTopicPublisher.this.thisChannel = ch;
        if (oldChannel != null && oldChannel.isOpen()) {
          LOG.info("Closing old channel " + oldChannel.getId());
          oldChannel.close().awaitUninterruptibly();
        }
      }
    }

    public Channel connect() throws Exception {
      Channel channel = null;
      try {
        LOG.info("Connecting to scribe host:" + host 
            + " port:" + port);
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,
            port));
        channel =
            future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess()) {
          LOG.info("Could not connect to Scribe");
          if (future.getCause() instanceof Throwable) {
            throw new RuntimeException(future.getCause());
          } else {
            throw (Exception)future.getCause();
          }
        } else {
          LOG.info("Connected to Scribe");
          setChannel(channel);
          return channel;
        }
      } catch (Exception e) {
        throw e;
      }
    }
  }

  public void init(String topic, String host, int port, int backoffSeconds,
      int timeoutSeconds, TimingAccumulator stats, boolean enableRetries,
      boolean resendOnAckLost, long sleepInterval, int msgQueueSize,
      int ackQueueSize, int numDrainsOnClose) {
    this.topic = topic;
    this.stats = stats;
    this.host = host;
    this.port = port;
    this.enabledRetries = enableRetries;
    this.resendOnAckLost = resendOnAckLost;
    this.sleepInterval = sleepInterval;

    this.toBeSent = new LinkedBlockingQueue<Message>(msgQueueSize);
    this.toBeAcked = new LinkedBlockingQueue<Message>(ackQueueSize);
    this.numDrainsOnClose = numDrainsOnClose;

    bootstrap = new ClientBootstrap(NettyEventCore.getInstance().getFactory());

    ChannelSetter chs = new ChannelSetter();
    handler = new ScribeHandler(stats, chs, backoffSeconds, timer, this);
    ChannelPipelineFactory cfactory = new ScribePipelineFactory(handler,
        timeoutSeconds, timer);
    bootstrap.setPipelineFactory(cfactory);
    try {
      chs.connect();
    } catch (Exception ioe) {
      LOG.info("Could not intialize the connection, scheduling reconnect");
      handler.setInited();
      handler.scheduleReconnect();
    }
    handler.setInited();
    senderThread = new Thread(new AsyncSender());
    senderThread.start();    
  }

  protected void publish(Message m) {
    addToSend(m);
    trySending();
  }

  private void addToSend(Message m) {
    synchronized (toBeSent) {
      if (toBeSent.remainingCapacity() == 0) {
        LOG.warn("Messages to be sent Queue is full," +
            " dropping the message");
        stats.accumulateOutcomeWithDelta(Outcome.LOST, 0);
        return;
      }
      toBeSent.add(m);
    }
  }

  private boolean isSendQueueEmpty() {
    synchronized (toBeSent) {
      return toBeSent.isEmpty();
    }
  }

  boolean isAckQueueEmpty() {
    synchronized (toBeAcked) {
      return toBeAcked.isEmpty();
    }
  }

  void trySending() {
    if (isSendQueueEmpty()) {
      return;
    }
    if (isChannelConnected()) {
      if (isChannelWritable()) {
        synchronized (toBeSent) {
          while (toBeSent.peek() != null && send(toBeSent.peek())) {
            toBeSent.remove();
          }
        }
      }
    } else {
      suggestReconnect(); 
    }
  }

  private boolean send(Message m) {
    synchronized (toBeAcked) {
      if (toBeAcked.remainingCapacity() > 0) {
        toBeAcked.add(m.clone());
        ScribeBites.publish(thisChannel, topic, m);
        return true;
      } else {
        LOG.info("Could not send earlier messages successfully, not" +
            " sending right now.");
      }
    }
    return false;
  }

  private class AsyncSender implements Runnable {
    @Override
    public void run() {
      while (!stopped && !Thread.interrupted()) {
        trySending();
        try {
          Thread.sleep(sleepInterval);
        } catch (InterruptedException ie) {
          LOG.info("Async sender interrupted. Exiting");
        }
      }
    }
  }

  private boolean isChannelConnected() {
    if (thisChannel == null) {
      LOG.info("Channel is not initialized yet, not sending right now");
      return false;
    }
    if (!thisChannel.isConnected()) {
      LOG.info("Channel is not connected, not sending right now");
      return false;
    }
    return true;
  }

  private boolean isChannelWritable() {
    if (reconnectionInProgress) {
      LOG.info("Reconnection in progress, not sending right now");
      return false;
    }
    if (!thisChannel.isWritable()) {
      LOG.info("Channel is not writable, not sending right now");
      return false;
    }
    return true;
  }

  private void suggestReconnect() {
    handler.scheduleReconnect();
  }

  private void drainAll()  {
    LOG.info("Draining all the messages");
    int numRetries = 0;
    while (true) {
      trySending();
      if (isSendQueueEmpty() && isAckQueueEmpty()) {
        break;
      }
      if ((numDrainsOnClose != -1 && numRetries > numDrainsOnClose)) {
        LOG.info("Dropping messages as channel is not connected or number of" +
            " retries exhausted");
        emptyAckQueue();
        emptyMsgQueue();
      }
      numRetries++;
      try {
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
        LOG.info("Draining inturrupted. Exiting");
        return;
      }
    }
  }

  void prepareReconnect() {
    reconnectionInProgress = true;
    emptyAckQueue();
  }

  void doneReconnect() {
    reconnectionInProgress = false;
  }

  synchronized void emptyAckQueue() {
    if (resendOnAckLost) {
      synchronized (toBeSent) {
        synchronized (toBeAcked) {
          toBeSent.addAll(toBeAcked);
          toBeAcked.clear();
        }
      }
    } else {
      synchronized (toBeAcked) {
        if (!toBeAcked.isEmpty()) {
          LOG.warn("Emptying ack queue of size:" + toBeAcked.size());
        }
        while (!toBeAcked.isEmpty()) {
          toBeAcked.remove();
          stats.accumulateOutcomeWithDelta(Outcome.GRACEFUL_FAILURE, 0);
        }
      }
    }
  }

  synchronized void emptyMsgQueue() {
    synchronized (toBeSent) {
      if (!toBeSent.isEmpty()) {
        LOG.warn("Emptying message queue of size:" + toBeSent.size());
      }
      while (!toBeSent.isEmpty()) {
        toBeSent.remove();
        stats.accumulateOutcomeWithDelta(Outcome.LOST, 0);
      }
    }
  }

  public void close() {
    stopped = true;
    if (senderThread != null) {
      senderThread.interrupt();
      try {
        senderThread.join();
      } catch (InterruptedException e) {
        LOG.info("join on sender Thread interrupted");
      }
    }
    drainAll();
    LOG.info("Closing the channel");
    handler.prepareClose();
    if (thisChannel != null) {
      thisChannel.close().awaitUninterruptibly();
    }
    timer.stop();
    NettyEventCore.getInstance().releaseFactory();
  }

  void ack(ResultCode success) {
    Message m;
    synchronized (toBeAcked) {
      m = toBeAcked.remove();
    }
    if (success.getValue() == 0) {
      stats.accumulateOutcomeWithDelta(Outcome.SUCCESS, 0);
    } else {
      if (enabledRetries) {
        LOG.info("Could not send the message successfully, resending");
        addToSend(m);
        stats.accumulateOutcomeWithDelta(Outcome.RETRY, 0);
      } else {
        LOG.warn("Could not send the message successfully. Got TRY_LATER");
        stats.accumulateOutcomeWithDelta(Outcome.GRACEFUL_FAILURE, 0);
      }
    }
  }
}
