package com.inmobi.messaging.publisher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.stats.EmitterRegistry;
import com.inmobi.stats.StatsEmitter;
import com.inmobi.stats.StatsExposer;

/**
 * Abstract class implementing {@link MessagePublisher} interface.
 * 
 * Initializes {@link StatsEmitter} and {@link StatsExposer} with configuration
 * defined in file {@value MessagePublisherFactory#EMITTER_CONF_FILE_KEY}. If 
 * no such file exists, statistics will be disabled.
 */
public abstract class AbstractMessagePublisher implements MessagePublisher {

  private static final Logger LOG = LoggerFactory
      .getLogger(AbstractMessagePublisher.class);
  private final TimingAccumulator stats = new TimingAccumulator();
  private StatsEmitter emitter;
  private boolean statEnabled = false;
  private StatsExposer statExposer;
  public static final String HEADER_TOPIC = "topic";

  @Override
  public void publish(String topicName, Message m) {
    getStats().accumulateInvocation();
    // TODO: generate headers
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(HEADER_TOPIC, topicName);
    publish(headers, m);
  }

  protected abstract void publish(Map<String, String> headers, Message m);

  public TimingAccumulator getStats() {
    return stats;
  }

  protected boolean statEmissionEnabled() {
    return statEnabled;
  }

  StatsEmitter getStatsEmitter() {
    return emitter;
  }

  protected void init(ClientConfig config) throws IOException {
    try {
      String emitterConfig = config
          .getString(MessagePublisherFactory.EMITTER_CONF_FILE_KEY);
      if (emitterConfig == null) {
        LOG.warn("Stat emitter is disabled as config "
            + MessagePublisherFactory.EMITTER_CONF_FILE_KEY + " is not set in the config.");
        return;
      }
      emitter = EmitterRegistry.lookup(emitterConfig);
      final Map<String, String> contexts = new HashMap<String, String>();
      contexts.put("messaging_type", "application");
      statExposer = new StatsExposer() {

        @Override
        public Map<String, Number> getStats() {
          HashMap<String, Number> hash = new HashMap<String, Number>();
          hash.put("cumulativeNanoseconds", stats.getCumulativeNanoseconds());
          hash.put("invocationCount", stats.getInvocationCount());
          hash.put("successCount", stats.getSuccessCount());
          hash.put("unhandledExceptionCount",
              stats.getUnhandledExceptionCount());
          hash.put("gracefulTerminates", stats.getGracefulTerminates());
          hash.put("inFlight", stats.getInFlight());
          return hash;
        }

        @Override
        public Map<String, String> getContexts() {
          return contexts;
        }
      };
      emitter.add(statExposer);
      statEnabled = true;
    } catch (Exception e) {
      throw new IOException("Couldn't find or initialize the configured stats" +
      		" emitter", e);
    }
  }

  @Override
  public void close() {
    if (emitter != null) {
      emitter.remove(statExposer);
    }
  }
}
