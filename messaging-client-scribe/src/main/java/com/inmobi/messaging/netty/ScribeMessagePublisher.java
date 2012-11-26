package com.inmobi.messaging.netty;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;

public class ScribeMessagePublisher extends AbstractMessagePublisher implements
    ScribePublisherConfiguration {
  public static final Log LOG = LogFactory.getLog(ScribeMessagePublisher.class);

  private String host;
  private int port;
  private int backoffSeconds;
  private int timeoutSeconds;
  private long asyncSleepInterval;
  private boolean resendOnAckLost;
  private boolean enableRetries;
  private int msgQueueSize;
  private int ackQueueSize;
  private int numDrainsOnClose;

  private Map<String, ScribeTopicPublisher> scribeConnections = new
      HashMap<String, ScribeTopicPublisher>();
  @Override
  public void init(ClientConfig config) throws IOException {
    super.init(config);
    init(config.getString(hostNameConfig, DEFAULT_HOST),
        config.getInteger(portConfig, DEFAULT_PORT),
        config.getInteger(backOffSecondsConfig, DEFAULT_BACKOFF),
        config.getInteger(timeoutSecondsConfig, DEFAULT_TIMEOUT),
        config.getBoolean(retryConfig, DEFAULT_ENABLE_RETRIES),
        config.getBoolean(resendAckLostConfig, DEFAULT_RESEND_ACKLOST),
        config.getLong(asyncSenderSleepMillis, DEFAULT_ASYNC_SENDER_SLEEP),
        config.getInteger(messageQueueSizeConfig, DEFAULT_MSG_QUEUE_SIZE),
        config.getInteger(ackQueueSizeConfig, DEFAULT_ACK_QUEUE_SIZE),
        config.getInteger(drainRetriesOnCloseConfig, DEFAULT_NUM_DRAINS_ONCLOSE)
        );
  }

  public void init(String host, int port, int backoffSeconds, int timeout,
      boolean enableRetries, boolean resendOnAckLost, long sleepInterval,
      int msgQueueSize, int ackQueueSize, int numDrainsOnClose) {
    this.host = host;
    this.port = port;
    this.backoffSeconds = backoffSeconds;
    this.timeoutSeconds = timeout;
    this.enableRetries = enableRetries;
    this.resendOnAckLost = resendOnAckLost;
    this.asyncSleepInterval = sleepInterval;
    this.msgQueueSize = msgQueueSize;
    this.ackQueueSize = ackQueueSize;
    this.numDrainsOnClose = numDrainsOnClose;
    LOG.info("Initialized ScribeMessagePublisher with host:" + host + " port:" +
        + port + " backoffSeconds:" + backoffSeconds + " timeoutSeconds:"
        + timeoutSeconds + " enableRetries:" + enableRetries +
        " resendOnAckLost:" + resendOnAckLost + "asyncSleepInterval:"
        + asyncSleepInterval + "msgQueueSize:" + msgQueueSize
        + "ackQueueSize:" + ackQueueSize + "numDrainsOnClose:" 
        + numDrainsOnClose);
  }

  protected void initTopic(String topic, TimingAccumulator stats) {
    super.initTopic(topic, stats);
    if (scribeConnections.get(topic) == null) {
      ScribeTopicPublisher connection = new ScribeTopicPublisher();
      scribeConnections.put(topic, connection);
      connection.init(topic, host, port, backoffSeconds, timeoutSeconds,stats,
          enableRetries, resendOnAckLost, asyncSleepInterval, msgQueueSize,
          ackQueueSize, numDrainsOnClose);
    }
  }

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    String topic = headers.get(HEADER_TOPIC);
    scribeConnections.get(topic).publish(m);
  }

  public void close() {
    for (ScribeTopicPublisher connection : scribeConnections.values()) {
      connection.close();
    }
<<<<<<< HEAD
    super.close();
=======
  }


  static class ScribeTopicPublisher {
    private final Timer timer = new HashedWheelTimer();

    private ClientBootstrap bootstrap;
    private volatile Channel ch = null;
    private String topic;
    private String host;
    private int port;
    private TimingAccumulator stats;

    /**
     * This is meant to be a way for async callbacks to set the channel on a
     * successful connection
     * 
     * Java does not have pointers to pointers. So have to resort to sending in a
     * wrapper object that knows to update our pointer
     */
    class ChannelSetter {
      final int maxConnectionRetries;
      ChannelSetter(int maxConns) {
        maxConnectionRetries = maxConns;
      }

      public Channel getCurrentChannel() {
        return ScribeTopicPublisher.this.ch;
      }

      public void setChannel(Channel ch) {
        Channel oldChannel = ScribeTopicPublisher.this.ch;
        if (ch != oldChannel) {
          if (oldChannel != null && oldChannel.isOpen()) {
            LOG.info("Closing old channel " + oldChannel.getId());
            oldChannel.close().awaitUninterruptibly();
          }
          LOG.info("setting channel to " + ch.getId());
          ScribeTopicPublisher.this.ch = ch;
        }
      }

      public Channel connect() throws IOException {
        int numRetries = 0;
        Channel channel = null;
        while (true) {
          try {
            LOG.info("Connecting to scribe host:" + host + " port:" + port);
            ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,
                port));
            channel =
                future.awaitUninterruptibly().getChannel();
            LOG.info("Connected to Scribe");
            setChannel(channel);
            if (!future.isSuccess()) {
              bootstrap.releaseExternalResources();
              throw new IOException(future.getCause());
            } else {
              return channel;
            }
          } catch (IOException e) {
            numRetries++;
            if (numRetries >= maxConnectionRetries) {
              throw e;
            }
            LOG.warn("Got exception while connecting. Retrying", e);
          }
        }
      }
    }

    public void init(String topic, String host, int port, int backoffSeconds,
        int timeoutSeconds, int maxConnectionRetries, TimingAccumulator stats)
            throws IOException {
      this.topic = topic;
      this.stats = stats;
      this.host = host;
      this.port = port;
      bootstrap = new ClientBootstrap(NettyEventCore.getInstance().getFactory());

      ChannelSetter chs = new ChannelSetter(maxConnectionRetries);
      ScribeHandler handler = new ScribeHandler(stats, chs,
          backoffSeconds, timer);
      ChannelPipelineFactory cfactory = new ScribePipelineFactory(handler,
          timeoutSeconds, timer);
      bootstrap.setPipelineFactory(cfactory);
      chs.connect();
      handler.setInited();
    }

    protected void publish(Message m) {
      if (ch != null) {
        ScribeBites.publish(ch, topic, m);
      } else {
        suggestReconnect();
      }
    }

    private void suggestReconnect() {
      LOG.warn("Suggesting reconnect as channel is null");
      long startTime = stats.getStartTime();
      stats.accumulateOutcome(Outcome.UNHANDLED_FAILURE, startTime);
      // TODO: logic for triggering reconnect
    }

    public void close() {
      if (ch != null) {
        ch.close().awaitUninterruptibly();
      }
      timer.stop();
      NettyEventCore.getInstance().releaseFactory();
    }
>>>>>>> 8308660be5f13041ae893f404ecaf8ffb2d775e1
  }
}
