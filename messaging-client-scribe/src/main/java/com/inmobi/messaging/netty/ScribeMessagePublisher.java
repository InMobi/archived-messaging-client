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
    super.close();
  }
}
