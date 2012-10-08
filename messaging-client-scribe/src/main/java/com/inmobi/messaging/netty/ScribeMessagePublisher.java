package com.inmobi.messaging.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;

public class ScribeMessagePublisher extends AbstractMessagePublisher {
  private static final Log LOG = LogFactory.getLog(ScribeMessagePublisher.class);

  public static final String maxConnectionRetriesConfig =
      "scribe.maxconnectionretries";
  public static final int DEFAULT_MAX_CONNECTION_RETRIES = 3;

  private String host;
  private int port;
  private int backoffSeconds;
  private int timeoutSeconds;
  private int maxConnectionRetries;

  private Map<String, ScribeTopicPublisher> scribeConnections = new
      HashMap<String, ScribeTopicPublisher>();
  @Override
  public void init(ClientConfig config) throws IOException {
    super.init(config);
    init(config.getString("scribe.host", "localhost"),
         config.getInteger("scribe.port", 1111),
         config.getInteger("scribe.backoffSeconds", 5),
         config.getInteger("scribe.timeoutSeconds", 5),
         config.getInteger(maxConnectionRetriesConfig,
        DEFAULT_MAX_CONNECTION_RETRIES));
  }

  public void init(String host, int port, int backoffSeconds, int timeout,
      int maxConnectionRetries) {
    this.host = host;
    this.port = port;
    this.backoffSeconds = backoffSeconds;
    this.timeoutSeconds = timeout;
    this.maxConnectionRetries = maxConnectionRetries;
    LOG.info("Initialized ScribeMessagePublisher with host:" + host + " port:" +
        + port + " backoffSeconds:" + backoffSeconds + " timeoutSeconds:"
        + timeoutSeconds + " maxConnectionRetries:" + maxConnectionRetries);
  }

  protected void initTopic(String topic, TimingAccumulator stats)
      throws IOException {
    super.initTopic(topic, stats);
    if (scribeConnections.get(topic) == null) {
      ScribeTopicPublisher connection = new ScribeTopicPublisher();
      try {
        connection.init(topic, host, port, backoffSeconds, timeoutSeconds,
          maxConnectionRetries, stats);
      } catch (IOException ioe) {
        connection.close();
        throw ioe;
      }
      scribeConnections.put(topic, connection);
    }
  }

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    String topic = headers.get(HEADER_TOPIC);
    scribeConnections.get(topic).publish(m);
  }

  public void close() {
    super.close();
    for (ScribeTopicPublisher connection : scribeConnections.values()) {
      connection.close();
    }
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
  }
}
