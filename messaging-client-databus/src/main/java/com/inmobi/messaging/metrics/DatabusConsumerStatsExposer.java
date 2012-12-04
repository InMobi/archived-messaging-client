package com.inmobi.messaging.metrics;

import java.util.Map;

import com.inmobi.messaging.consumer.BaseMessageConsumerStatsExposer;

public class DatabusConsumerStatsExposer extends
    BaseMessageConsumerStatsExposer {

  public static String CONSUMER_NUMBER_CONTEXT = "consumerNumber";

  Integer consumerNumber;
  public DatabusConsumerStatsExposer(String topicName, String consumerName,
      int consumerNumber) {
    super(topicName, consumerName);
    this.consumerNumber = consumerNumber;
  }

  protected void addToContextsMap(Map<String, String> contexts) {
    super.addToContextsMap(contexts);
    contexts.put(CONSUMER_NUMBER_CONTEXT, consumerNumber.toString());
  }
}
